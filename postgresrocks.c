#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attribute.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "parser/parse_relation.h"
#include "access/table.h"
#include "utils/varlena.h"
#include "access/tupdesc.h"
#include "utils/datum.h"
#include "utils/syscache.h"
#include "storage/itemptr.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/attnum.h"
#include "storage/bufpage.h"

#include <rocksdb/c.h>
#include <string.h>
#include <stdio.h>

PG_MODULE_MAGIC;

/* Global RocksDB instance */
static rocksdb_t* rocks_db = NULL;
static rocksdb_options_t* rocks_options = NULL;
static rocksdb_readoptions_t* rocks_read_options = NULL;
static rocksdb_writeoptions_t* rocks_write_options = NULL;

/* Hash table for tracking speculative insertions */
static HTAB* speculative_insertions = NULL;

/* Global scan counter removed - was unused */

/* Structure to track speculative insertions */
typedef struct SpeculativeInsertEntry
{
    uint32 specToken;        /* Hash key */
    Oid table_oid;          /* Table OID */
    uint64 rowid;           /* Row ID */
    uint32 chunk_id;        /* Chunk ID */
    uint32 chunk_offset;    /* Chunk offset */
    char *key;              /* RocksDB key */
    size_t key_len;         /* Key length */
    char *data;             /* Serialized tuple data */
    size_t data_len;        /* Data length */
} SpeculativeInsertEntry;

/* Remove scan state structure - using simple global counter for now */

/* Storage format design - ROW-BASED STORAGE:
 * Key format: 
 * - Row data: "row_<oid>_<rowid>" -> [serialized_row_data]
 * - Metadata: "meta_<oid>_info" -> [row_count, col_count]
 * 
 * For each table, we store:
 * - "meta_<oid>_info" -> table metadata (row count, column count)
 * - "row_<oid>_<rowid>" -> complete serialized row data
 */

/* Row storage constants */
#define MAX_ROW_SIZE 8192  /* Maximum size for a serialized row */

/* Row data structure */
typedef struct RowData
{
    uint64 rowid;
    char *data;
    size_t data_len;
} RowData;

/* Table metadata structure */
typedef struct TableMetadata
{
    uint64 row_count;
    uint32 column_count;
    Oid column_types[MaxAttrNumber];
} TableMetadata;

/* Row header structure for serialized data */
typedef struct RowHeader
{
    uint32 natts;      /* Number of attributes */
    uint32 data_len;   /* Length of row data */
} RowHeader;

/* Helper function to create row data key */
static char *
make_row_key(Oid table_oid, uint64 rowid, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "row_%u_%lu", table_oid, rowid);
    return key;
}

/* Helper function to create key prefix for table scanning */
static char *
make_row_prefix(Oid table_oid, size_t *prefix_len)
{
    char *prefix = palloc(32);
    *prefix_len = snprintf(prefix, 32, "row_%u_", table_oid);
    return prefix;
}

/* Helper function to create metadata key */
static char *
make_metadata_key(Oid table_oid, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "meta_%u_info", table_oid);
    return key;
}

/* Helper function to serialize a complete row */
static char *
serialize_row(TupleTableSlot *slot, size_t *data_len)
{
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int natts = tupdesc->natts;
    char *data, *ptr;
    size_t total_size;
    int i;
    RowHeader header;
    
    /* Calculate total size needed */
    total_size = sizeof(RowHeader) + natts * sizeof(bool); /* header + null bitmap */
    
    for (i = 0; i < natts; i++) {
        if (!slot->tts_isnull[i]) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            switch (attr->atttypid) {
                case INT4OID:
                    total_size += sizeof(int32);
                    break;
                case INT8OID:
                    total_size += sizeof(int64);
                    break;
                case TEXTOID:
                case VARCHAROID: {
                    text *txt = DatumGetTextP(slot->tts_values[i]);
                    total_size += sizeof(int32) + VARSIZE(txt) - VARHDRSZ;
                    break;
                }
                default:
                    elog(ERROR, "Unsupported data type during serialization: %u", attr->atttypid);
            }
        }
    }
    
    /* Allocate buffer */
    data = palloc(total_size);
    ptr = data;
    
    /* Write header */
    header.natts = natts;
    header.data_len = total_size;
    memcpy(ptr, &header, sizeof(RowHeader));
    ptr += sizeof(RowHeader);
    
    /* Write null bitmap */
    for (i = 0; i < natts; i++) {
        *((bool*)ptr) = slot->tts_isnull[i];
        ptr += sizeof(bool);
    }
    
    /* Write attribute values */
    for (i = 0; i < natts; i++) {
        if (!slot->tts_isnull[i]) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            switch (attr->atttypid) {
                case INT4OID: {
                    int32 val = DatumGetInt32(slot->tts_values[i]);
                    memcpy(ptr, &val, sizeof(int32));
                    ptr += sizeof(int32);
                    break;
                }
                case INT8OID: {
                    int64 val = DatumGetInt64(slot->tts_values[i]);
                    memcpy(ptr, &val, sizeof(int64));
                    ptr += sizeof(int64);
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    text *txt = DatumGetTextP(slot->tts_values[i]);
                    int32 len = VARSIZE(txt) - VARHDRSZ;
                    memcpy(ptr, &len, sizeof(int32));
                    ptr += sizeof(int32);
                    memcpy(ptr, VARDATA(txt), len);
                    ptr += len;
                    break;
                }
                default:
                    elog(ERROR, "Unsupported data type during serialization: %u", attr->atttypid);
            }
        }
    }
    
    *data_len = total_size;
    return data;
}

/* Helper function to deserialize a complete row */
static void
deserialize_row(char *data, size_t data_len, TupleTableSlot *slot)
{
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    char *ptr = data;
    RowHeader *header;
    bool *null_bitmap;
    int i;
    
    /* Read header */
    header = (RowHeader *) ptr;
    ptr += sizeof(RowHeader);
    
    /* Verify header */
    if (header->natts != tupdesc->natts) {
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u", 
             tupdesc->natts, header->natts);
    }
    
    /* Read null bitmap */
    null_bitmap = (bool *) ptr;
    ptr += header->natts * sizeof(bool);
    
    /* Clear the slot */
    ExecClearTuple(slot);
    
    /* Read attribute values */
    for (i = 0; i < header->natts; i++) {
        slot->tts_isnull[i] = null_bitmap[i];
        
        if (!null_bitmap[i]) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            switch (attr->atttypid) {
                case INT4OID: {
                    int32 val;
                    memcpy(&val, ptr, sizeof(int32));
                    slot->tts_values[i] = Int32GetDatum(val);
                    ptr += sizeof(int32);
                    break;
                }
                case INT8OID: {
                    int64 val;
                    memcpy(&val, ptr, sizeof(int64));
                    slot->tts_values[i] = Int64GetDatum(val);
                    ptr += sizeof(int64);
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    int32 len;
                    text *txt;
                    
                    memcpy(&len, ptr, sizeof(int32));
                    ptr += sizeof(int32);
                    
                    txt = (text *) palloc(VARHDRSZ + len);
                    SET_VARSIZE(txt, VARHDRSZ + len);
                    memcpy(VARDATA(txt), ptr, len);
                    ptr += len;
                    
                    slot->tts_values[i] = PointerGetDatum(txt);
                    break;
                }
                default:
                    elog(ERROR, "Unsupported data type during deserialization: %u", attr->atttypid);
            }
        } else {
            slot->tts_values[i] = (Datum) 0;
        }
    }
    
    /* Set slot as valid */
    slot->tts_nvalid = header->natts;
    ExecStoreVirtualTuple(slot);
}

/* Custom scan descriptor for row storage */
typedef struct RocksScanDesc
{
    TableScanDescData rs_base;
    char *key_prefix;
    size_t key_prefix_len;
    uint64 current_rowid;
    uint64 total_rows;
    bool started;
} RocksScanDesc;

/* Function to initialize speculative insertions hash table */
static void
init_speculative_hash(void)
{
    HASHCTL info;
    
    if (speculative_insertions != NULL)
        return; /* Already initialized */
    
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(uint32);
    info.entrysize = sizeof(SpeculativeInsertEntry);
    info.hcxt = TopMemoryContext;
    
    speculative_insertions = hash_create("Speculative Insertions",
                                        256, /* initial size */
                                        &info,
                                        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Remove scan hash initialization */

/* Function to initialize RocksDB */
static void
init_rocksdb(void)
{
    char *err = NULL;
    
    if (rocks_db != NULL)
        return; /* Already initialized */
    
    rocks_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(rocks_options, 1);
    rocksdb_options_set_compression(rocks_options, rocksdb_snappy_compression);
    
    rocks_read_options = rocksdb_readoptions_create();
    rocks_write_options = rocksdb_writeoptions_create();
    
    /* Open database in PostgreSQL data directory */
    rocks_db = rocksdb_open(rocks_options, "postgresrocks_data", &err);
    
    if (err != NULL) {
        elog(ERROR, "Failed to open RocksDB: %s", err);
        free(err);
    }
    
    /* Initialize speculative insertions tracking */
    init_speculative_hash();
}

/* Get or create table metadata */
static TableMetadata*
get_table_metadata(Oid table_oid, bool create_if_missing)
{
    char *key, *value, *err = NULL;
    size_t key_len, value_len;
    TableMetadata *meta = palloc0(sizeof(TableMetadata));
    
    init_rocksdb();
    
    if (!rocks_db) {
        elog(ERROR, "RocksDB not initialized");
    }
    
    key = make_metadata_key(table_oid, &key_len);
    
    value = rocksdb_get(rocks_db, rocks_read_options, key, key_len, &value_len, &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB get error: %s", err);
        free(err);
    }
    
    if (value != NULL && value_len >= sizeof(TableMetadata)) {
        /* Load existing metadata */
        memcpy(meta, value, sizeof(TableMetadata));
        free(value);
    } else if (create_if_missing) {
        /* Initialize new table metadata */
        meta->row_count = 0;
        meta->column_count = 0;
        
        /* Store the metadata */
        rocksdb_put(rocks_db, rocks_write_options, key, key_len,
                    (char*)meta, sizeof(TableMetadata), &err);
        
        if (err != NULL) {
            elog(ERROR, "RocksDB put error: %s", err);
            free(err);
        }
    } else {
        /* Return NULL if not found and not creating */
        pfree(meta);
        meta = NULL;
    }
    
    pfree(key);
    return meta;
}

/* Update table metadata */
static void
update_table_metadata(Oid table_oid, TableMetadata *meta)
{
    char *key, *err = NULL;
    size_t key_len;
    
    key = make_metadata_key(table_oid, &key_len);
    
    rocksdb_put(rocks_db, rocks_write_options, key, key_len,
                (char*)meta, sizeof(TableMetadata), &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB put error: %s", err);
        free(err);
    }
    
    pfree(key);
}

/* Get next row ID for insertion */
static uint64
get_next_row_id(Oid table_oid)
{
    TableMetadata *meta = get_table_metadata(table_oid, true);
    uint64 rowid;
    
    if (!meta) {
        elog(ERROR, "Failed to get table metadata for table %u", table_oid);
    }
    
    rowid = meta->row_count + 1;
    
    /* Update row count */
    meta->row_count = rowid;
    
    /* Update metadata after each row for correctness */
    update_table_metadata(table_oid, meta);
    elog(LOG, "Updated metadata: table_oid=%u, row_count=%lu", table_oid, meta->row_count);
    
    pfree(meta);
    return rowid;
}

/* Old column-based functions removed - using row-based storage now */

/* Old column chunk functions removed */

/* Old column chunk deserialize functions removed */

/* deserialize_stored_tuple function removed - was unused in row-based storage */

/* Forward declarations */
static const TupleTableSlotOps *rocks_slot_callbacks(Relation relation);
static TableScanDesc rocks_beginscan(Relation relation, Snapshot snapshot,
                                     int nkeys, ScanKey key,
                                     ParallelTableScanDesc parallel_scan,
                                     uint32 flags);
static void rocks_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
                        bool allow_strat, bool allow_sync, bool allow_pagemode);
static bool rocks_getnextslot(TableScanDesc sscan, ScanDirection direction,
                             TupleTableSlot *slot);
static void rocks_endscan(TableScanDesc sscan);
static void rocks_tuple_insert(Relation relation, TupleTableSlot *slot,
                              CommandId cid, int options,
                              BulkInsertState bistate);
static void rocks_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                          CommandId cid, int options,
                                          BulkInsertState bistate,
                                          uint32 specToken);
static void rocks_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                           uint32 specToken, bool succeeded);
static void rocks_multi_insert(Relation relation, TupleTableSlot **slots,
                              int ntuples, CommandId cid, int options,
                              BulkInsertState bistate);
static TM_Result rocks_tuple_delete(Relation relation, ItemPointer tid,
                                   CommandId cid, Snapshot snapshot,
                                   Snapshot crosscheck, bool wait,
                                   TM_FailureData *tmfd, bool changingPart);
static TM_Result rocks_tuple_update(Relation relation, ItemPointer otid,
                                   TupleTableSlot *slot, CommandId cid,
                                   Snapshot snapshot, Snapshot crosscheck,
                                   bool wait, TM_FailureData *tmfd,
                                   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes);
static TM_Result rocks_tuple_lock(Relation relation, ItemPointer tid,
                                 Snapshot snapshot, TupleTableSlot *slot,
                                 CommandId cid, LockTupleMode mode,
                                 LockWaitPolicy wait_policy, uint8 flags,
                                 TM_FailureData *tmfd);
static void rocks_finish_bulk_insert(Relation relation, int options);
static void rocks_relation_set_new_filelocator(Relation rel,
                                              const RelFileLocator *newrlocator,
                                              char persistence,
                                              TransactionId *freezeXid,
                                              MultiXactId *minmulti);
static void rocks_relation_nontransactional_truncate(Relation rel);
static void rocks_relation_copy_data(Relation rel, const RelFileLocator *newrlocator);
static void rocks_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                           Relation OldIndex, bool use_sort,
                                           TransactionId OldestXmin,
                                           TransactionId *xid_cutoff,
                                           MultiXactId *multi_cutoff,
                                           double *num_tuples,
                                           double *tups_vacuumed,
                                           double *tups_recently_dead);
static void rocks_relation_vacuum(Relation onerel, VacuumParams *params,
                                 BufferAccessStrategy bstrategy);
static bool rocks_scan_analyze_next_block(TableScanDesc scan,
                                         ReadStream *stream);
static bool rocks_scan_analyze_next_tuple(TableScanDesc scan,
                                         TransactionId OldestXmin,
                                         double *liverows, double *deadrows,
                                         TupleTableSlot *slot);
static double rocks_index_build_range_scan(Relation tablerel, Relation indexrel,
                                          IndexInfo *indexInfo,
                                          bool allow_sync, bool anyvisible,
                                          bool progress, BlockNumber start_blockno,
                                          BlockNumber numblocks,
                                          IndexBuildCallback callback,
                                          void *callback_state,
                                          TableScanDesc scan);
static void rocks_index_validate_scan(Relation tablerel, Relation indexrel,
                                     IndexInfo *indexInfo, Snapshot snapshot,
                                     ValidateIndexState *state);
static uint64 rocks_relation_size(Relation rel, ForkNumber forkNumber);
static bool rocks_relation_needs_toast_table(Relation rel);
static void rocks_relation_estimate_size(Relation rel, int32 *attr_widths,
                                        BlockNumber *pages, double *tuples,
                                        double *allvisfrac);


/* Stub implementations removed - using actual implementations now */

/* All stub functions removed - using actual implementations */

/* Table access method routine structure - complete PostgreSQL 17 compatible version */
static const TableAmRoutine rocks_methods = {
    .type = T_TableAmRoutine,

    /* Slot related callbacks */
    .slot_callbacks = rocks_slot_callbacks,

    /* Table scan callbacks */
    .scan_begin = rocks_beginscan,
    .scan_end = rocks_endscan,
    .scan_rescan = rocks_rescan,
    .scan_getnextslot = rocks_getnextslot,

    /* TID range scan callbacks - MISSING FIELDS! */
    .scan_set_tidrange = NULL,
    .scan_getnextslot_tidrange = NULL,

    /* Parallel table scan callbacks - MISSING FIELDS! */
    .parallelscan_estimate = NULL,
    .parallelscan_initialize = NULL,
    .parallelscan_reinitialize = NULL,

    /* Index scan callbacks - MISSING FIELDS! */
    .index_fetch_begin = NULL,
    .index_fetch_reset = NULL,
    .index_fetch_end = NULL,
    .index_fetch_tuple = NULL,

    /* Non-modifying operations on individual tuples - MISSING FIELDS! */
    .tuple_fetch_row_version = NULL,
    .tuple_tid_valid = NULL,
    .tuple_get_latest_tid = NULL,
    .tuple_satisfies_snapshot = NULL,
    .index_delete_tuples = NULL,

    /* Manipulations of physical tuples */
    .tuple_insert = rocks_tuple_insert,
    .tuple_insert_speculative = rocks_tuple_insert_speculative,
    .tuple_complete_speculative = rocks_tuple_complete_speculative,
    .multi_insert = rocks_multi_insert,
    .tuple_delete = rocks_tuple_delete,
    .tuple_update = rocks_tuple_update,
    .tuple_lock = rocks_tuple_lock,
    .finish_bulk_insert = rocks_finish_bulk_insert,

    /* DDL related functionality */
    .relation_set_new_filelocator = rocks_relation_set_new_filelocator,
    .relation_nontransactional_truncate = rocks_relation_nontransactional_truncate,
    .relation_copy_data = rocks_relation_copy_data,
    .relation_copy_for_cluster = rocks_relation_copy_for_cluster,
    .relation_vacuum = rocks_relation_vacuum,
    .scan_analyze_next_block = rocks_scan_analyze_next_block,
    .scan_analyze_next_tuple = rocks_scan_analyze_next_tuple,
    .index_build_range_scan = rocks_index_build_range_scan,
    .index_validate_scan = rocks_index_validate_scan,

    /* Miscellaneous functions */
    .relation_size = rocks_relation_size,
    .relation_needs_toast_table = rocks_relation_needs_toast_table,

    /* MISSING FIELDS! */
    .relation_toast_am = NULL,
    .relation_fetch_toast_slice = NULL,

    /* Planner related functions - MISSING FIELDS! */
    .relation_estimate_size = rocks_relation_estimate_size,

    /* Executor related functions - MISSING FIELDS! */
    .scan_bitmap_next_block = NULL,
    .scan_bitmap_next_tuple = NULL,
    .scan_sample_next_block = NULL,
    .scan_sample_next_tuple = NULL,
};

/* Entry point function */
PG_FUNCTION_INFO_V1(postgresrocks_tableam_handler);
Datum
postgresrocks_tableam_handler(PG_FUNCTION_ARGS)
{
    elog(LOG, "postgresrocks_tableam_handler called - returning TableAmRoutine");
    PG_RETURN_POINTER(&rocks_methods);
}

/* Custom function to read data from RocksDB and demonstrate SELECT functionality */
PG_FUNCTION_INFO_V1(postgresrocks_read_data);
Datum
postgresrocks_read_data(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int call_cntr;
    int max_calls;
    TupleDesc tupdesc;
    AttInMetadata *attinmeta;
    text *table_name_text;
    char *table_name;
    Oid table_oid;
    TableMetadata *meta;
    Oid namespaceId;
    RangeVar *relvar;
    
    /* Stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        
        /* Get table name from function argument */
        table_name_text = PG_GETARG_TEXT_P(0);
        table_name = text_to_cstring(table_name_text);
        
        /* Create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        
        /* Switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* Look up table OID by name using proper namespace resolution */
        /* Try searching in public namespace first */
        namespaceId = get_namespace_oid("public", true);
        if (namespaceId != InvalidOid) {
            table_oid = get_relname_relid(table_name, namespaceId);
        } else {
            table_oid = InvalidOid;
        }
        
        /* If not found in public, try the current search path */
        if (table_oid == InvalidOid) {
            /* Create a minimal RangeVar manually */
            relvar = palloc0(sizeof(RangeVar));
            relvar->schemaname = NULL;  /* Will use search_path */
            relvar->relname = pstrdup(table_name);
            relvar->inh = true;
            relvar->relpersistence = RELPERSISTENCE_PERMANENT;
            relvar->location = -1;
            
            /* Try to get the relation OID */
            table_oid = RangeVarGetRelid(relvar, NoLock, true);
            
            pfree(relvar->relname);
            pfree(relvar);
        }
        
        if (table_oid == InvalidOid) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                     errmsg("table \"%s\" does not exist", table_name)));
        }
        
        /* Initialize RocksDB */
        init_rocksdb();
        
        /* Get table metadata to find row count */
        meta = get_table_metadata(table_oid, false);
        max_calls = meta ? meta->row_count : 0;
        
        /* Store context info */
        funcctx->user_fctx = (void *)(uintptr_t)table_oid;
        funcctx->max_calls = max_calls;
        
        /* Build tuple descriptor for the output tuples */
        tupdesc = CreateTemplateTupleDesc(2);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id",
                          INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name",
                          TEXTOID, -1, 0);
        
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        
        if (meta) pfree(meta);
        MemoryContextSwitchTo(oldcontext);
    }
    
    /* Stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;
    table_oid = (Oid)(uintptr_t)funcctx->user_fctx;
    
    if (call_cntr < max_calls) {
        char **values;
        HeapTuple tuple;
        Datum result;
        uint64 rowid = call_cntr + 1;
        char *row_key;
        size_t row_key_len;
        char *row_data;
        size_t row_data_len;
        char *err = NULL;
        RowHeader *header;
        bool *null_bitmap;
        char *data_ptr;
        
        /* Get row data */
        row_key = make_row_key(table_oid, rowid, &row_key_len);
        row_data = rocksdb_get(rocks_db, rocks_read_options, row_key, row_key_len, &row_data_len, &err);
        
        if (err != NULL || row_data == NULL) {
            pfree(row_key);
            if (err) free(err);
            SRF_RETURN_DONE(funcctx);
        }
        
        /* Parse row header */
        header = (RowHeader *) row_data;
        null_bitmap = (bool *) (row_data + sizeof(RowHeader));
        data_ptr = row_data + sizeof(RowHeader) + header->natts * sizeof(bool);
        
        /* Build a tuple with actual data */
        values = (char **) palloc(2 * sizeof(char *));
        
        /* Extract ID (column 0) */
        if (null_bitmap[0]) {
            values[0] = pstrdup("NULL");
        } else {
            int32 id_value;
            memcpy(&id_value, data_ptr, sizeof(int32));
            values[0] = psprintf("%d", id_value);
            data_ptr += sizeof(int32);
        }
        
        /* Extract name (column 1) */
        if (header->natts > 1) {
            if (null_bitmap[1]) {
                values[1] = pstrdup("NULL");
            } else {
                int32 text_len;
                char *text_data;
                
                memcpy(&text_len, data_ptr, sizeof(int32));
                data_ptr += sizeof(int32);
                
                text_data = palloc(text_len + 1);
                memcpy(text_data, data_ptr, text_len);
                text_data[text_len] = '\0';
                values[1] = text_data;
            }
        } else {
            values[1] = pstrdup("");
        }
        
        /* Clean up */
        pfree(row_key);
        free(row_data);
        
        /* Build the tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);
        
        /* Make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);
        
        SRF_RETURN_NEXT(funcctx, result);
    } else {
        /* Do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

/* Slot callbacks - use heap tuple table slots for compatibility */
static const TupleTableSlotOps *
rocks_slot_callbacks(Relation relation)
{
    elog(LOG, "rocks_slot_callbacks called - START");
    if (relation) {
        elog(LOG, "rocks_slot_callbacks for relation %s", RelationGetRelationName(relation));
    } else {
        elog(LOG, "rocks_slot_callbacks called with NULL relation");
    }
    elog(LOG, "rocks_slot_callbacks returning TTSOpsHeapTuple");
    return &TTSOpsHeapTuple;
}

/* Scan operations */
static TableScanDesc
rocks_beginscan(Relation relation, Snapshot snapshot,
               int nkeys, ScanKey key,
               ParallelTableScanDesc parallel_scan,
               uint32 flags)
{
    RocksScanDesc *scan;
    Oid table_oid;
    TableMetadata *meta;
    
    elog(LOG, "rocks_beginscan called - creating scan with iterator");
    
    init_rocksdb();
    
    /* Use our custom scan structure */
    scan = (RocksScanDesc *) palloc0(sizeof(RocksScanDesc));
    scan->rs_base.rs_rd = relation;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_key = key;
    scan->rs_base.rs_flags = flags;
    
    /* Initialize scan state */
    table_oid = RelationGetRelid(relation);
    scan->key_prefix = make_row_prefix(table_oid, &scan->key_prefix_len);
    scan->current_rowid = 1;  /* Start from first row */
    scan->started = false;
    
    /* Get table metadata to know how many rows we have */
    meta = get_table_metadata(table_oid, false);
    if (meta) {
        scan->total_rows = meta->row_count;
        elog(LOG, "rocks_beginscan - found metadata: table_oid=%u, row_count=%lu", table_oid, meta->row_count);
        pfree(meta);
    } else {
        scan->total_rows = 0;
        elog(LOG, "rocks_beginscan - NO metadata found for table_oid=%u", table_oid);
    }
    
    elog(LOG, "rocks_beginscan completed - scan ready, total_rows=%lu", scan->total_rows);
    
    return (TableScanDesc) scan;
}

static void
rocks_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    elog(LOG, "rocks_rescan called");
    /* For basic scan, we don't need to do anything special */
}

/* Row-based scanning - no chunk loading needed */

static bool
rocks_getnextslot(TableScanDesc sscan, ScanDirection direction,
                 TupleTableSlot *slot)
{
    RocksScanDesc *scan = (RocksScanDesc *) sscan;
    Relation relation = scan->rs_base.rs_rd;
    Oid table_oid = RelationGetRelid(relation);
    char *row_key;
    size_t row_key_len;
    char *row_data, *err;
    size_t row_data_len;
    
    elog(LOG, "rocks_getnextslot called - current_rowid=%lu, total_rows=%lu", 
         scan->current_rowid, scan->total_rows);
    
    /* Check if we've scanned all rows */
    if (scan->current_rowid > scan->total_rows) {
        elog(LOG, "rocks_getnextslot - no more rows, returning false");
        return false;
    }
    
    /* Get row data for current row */
    row_key = make_row_key(table_oid, scan->current_rowid, &row_key_len);
    err = NULL;
    row_data = rocksdb_get(rocks_db, rocks_read_options, row_key, row_key_len, &row_data_len, &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB get error for row data: %s", err);
        free(err);
    }
    
    if (row_data == NULL) {
        elog(LOG, "rocks_getnextslot - row %lu not found, skipping", scan->current_rowid);
        scan->current_rowid++;
        pfree(row_key);
        return rocks_getnextslot(sscan, direction, slot);  /* Try next row */
    }
    
    elog(LOG, "rocks_getnextslot - found row %lu, data_len=%zu", 
         scan->current_rowid, row_data_len);
    
    /* Deserialize the complete row into the slot */
    deserialize_row(row_data, row_data_len, slot);
    
    /* Move to next row */
    scan->current_rowid++;
    
    /* Cleanup */
    pfree(row_key);
    free(row_data);
    
    elog(LOG, "rocks_getnextslot - successfully deserialized row %lu", scan->current_rowid - 1);
    return true;
}

static void
rocks_endscan(TableScanDesc sscan)
{
    RocksScanDesc *scan = (RocksScanDesc *) sscan;
    
    elog(LOG, "rocks_endscan called");
    if (scan) {
        /* Cleanup scan state */
        if (scan->key_prefix) {
            pfree(scan->key_prefix);
        }
        pfree(scan);
    }
}

/* Tuple modification operations */
static void
rocks_tuple_insert(Relation relation, TupleTableSlot *slot,
                  CommandId cid, int options,
                  BulkInsertState bistate)
{
    Oid table_oid = RelationGetRelid(relation);
    uint64 rowid;
    char *err = NULL;
    char *row_key;
    size_t row_key_len;
    char *row_data;
    size_t row_data_len;
    
    elog(LOG, "rocks_tuple_insert - table_oid=%u", table_oid);
    
    init_rocksdb();
    
    /* Ensure the slot is properly materialized */
    slot_getallattrs(slot);
    
    /* Get next row ID */
    rowid = get_next_row_id(table_oid);
    
    /* Serialize the complete row */
    row_data = serialize_row(slot, &row_data_len);
    
    /* Create row key */
    row_key = make_row_key(table_oid, rowid, &row_key_len);
    
    /* Store the serialized row in RocksDB */
    rocksdb_put(rocks_db, rocks_write_options, row_key, row_key_len,
                row_data, row_data_len, &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB put error for row data: %s", err);
        free(err);
    }
    
    elog(LOG, "rocks_tuple_insert - successfully stored row %lu, data_len=%zu", rowid, row_data_len);
    
    /* Cleanup */
    pfree(row_key);
    pfree(row_data);
}

static void
rocks_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                              CommandId cid, int options,
                              BulkInsertState bistate,
                              uint32 specToken)
{
    Oid table_oid = RelationGetRelid(relation);
    uint64 rowid;
    SpeculativeInsertEntry *entry;
    bool found;
    
    init_rocksdb();
    
    /* Ensure the slot is properly materialized */
    slot_getallattrs(slot);
    
    /* Get next row ID */
    rowid = get_next_row_id(table_oid);
    
    /* Store the speculative insertion in our hash table (not RocksDB yet) */
    entry = (SpeculativeInsertEntry *) hash_search(speculative_insertions,
                                                  &specToken,
                                                  HASH_ENTER,
                                                  &found);
    
    if (found) {
        /* Clean up previous entry if it exists */
        if (entry->key)
            pfree(entry->key);
        if (entry->data)
            pfree(entry->data);
    }
    
    /* Store the insertion data */
    entry->specToken = specToken;
    entry->table_oid = table_oid;
    entry->rowid = rowid;
    entry->chunk_id = 0;  /* Not used in row storage */
    entry->chunk_offset = 0;  /* Not used in row storage */
    
    /* Store row key */
    entry->key = make_row_key(table_oid, rowid, &entry->key_len);
    
    /* Serialize the complete row data for storage */
    entry->data = serialize_row(slot, &entry->data_len);
    
    /* 
     * Do NOT insert into RocksDB yet - this is speculative!
     * The actual insertion will happen in rocks_tuple_complete_speculative
     * if the speculation succeeds.
     */
}

static void
rocks_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                               uint32 specToken, bool succeeded)
{
    SpeculativeInsertEntry *entry;
    bool found;
    char *err = NULL;
    Oid table_oid;
    TableMetadata *meta;
    
    init_rocksdb();
    
    /* Find the speculative insertion entry */
    entry = (SpeculativeInsertEntry *) hash_search(speculative_insertions,
                                                  &specToken,
                                                  HASH_FIND,
                                                  &found);
    
    if (!found) {
        elog(WARNING, "Speculative insertion token %u not found", specToken);
        return;
    }
    
    if (succeeded) {
        /* 
         * Speculation succeeded - commit the insertion to RocksDB 
         */
        rocksdb_put(rocks_db, rocks_write_options, 
                   entry->key, entry->key_len,
                   entry->data, entry->data_len, &err);
        
        if (err != NULL) {
            elog(ERROR, "RocksDB put error during speculative completion: %s", err);
            free(err);
        }
        
        elog(LOG, "Speculative insertion succeeded for row %lu", entry->rowid);
    } else {
        /*
         * Speculation failed - we need to "undo" the row ID allocation
         * by decrementing the row count for this table
         */
        table_oid = entry->table_oid;
        meta = get_table_metadata(table_oid, false);
        
        if (meta && meta->row_count > 0) {
            meta->row_count--;
            update_table_metadata(table_oid, meta);
            pfree(meta);
        }
        
        elog(LOG, "Speculative insertion failed for row %lu", entry->rowid);
    }
    
    /* Clean up the speculative entry */
    if (entry->key)
        pfree(entry->key);
    if (entry->data)
        pfree(entry->data);
    
    /* Remove from hash table */
    hash_search(speculative_insertions, &specToken, HASH_REMOVE, &found);
}

static void
rocks_multi_insert(Relation relation, TupleTableSlot **slots,
                  int ntuples, CommandId cid, int options,
                  BulkInsertState bistate)
{
    int i;
    
    /* TODO: Implement multi-insert */
    for (i = 0; i < ntuples; i++)
    {
        rocks_tuple_insert(relation, slots[i], cid, options, bistate);
    }
}

static TM_Result
rocks_tuple_delete(Relation relation, ItemPointer tid,
                  CommandId cid, Snapshot snapshot,
                  Snapshot crosscheck, bool wait,
                  TM_FailureData *tmfd, bool changingPart)
{
    Oid table_oid = RelationGetRelid(relation);
    uint64 rowid;
    char *key, *err = NULL;
    size_t key_len;
    
    init_rocksdb();
    
    /* Extract row ID from ItemPointer */
    rowid = ItemPointerGetBlockNumber(tid) * BLCKSZ + ItemPointerGetOffsetNumber(tid);
    
    /* Create key for this row */
    key = make_row_key(table_oid, rowid, &key_len);
    
    /* Delete from RocksDB */
    rocksdb_delete(rocks_db, rocks_write_options, key, key_len, &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB delete error: %s", err);
        free(err);
        pfree(key);
        return TM_Deleted; /* Indicate failure */
    }
    
    pfree(key);
    return TM_Ok;
}

static TM_Result
rocks_tuple_update(Relation relation, ItemPointer otid,
                  TupleTableSlot *slot, CommandId cid,
                  Snapshot snapshot, Snapshot crosscheck,
                  bool wait, TM_FailureData *tmfd,
                  LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    /* TODO: Implement tuple update */
    return TM_Ok;
}

static TM_Result
rocks_tuple_lock(Relation relation, ItemPointer tid,
                Snapshot snapshot, TupleTableSlot *slot,
                CommandId cid, LockTupleMode mode,
                LockWaitPolicy wait_policy, uint8 flags,
                TM_FailureData *tmfd)
{
    /* TODO: Implement tuple locking */
    return TM_Ok;
}

static void
rocks_finish_bulk_insert(Relation relation, int options)
{
    /* TODO: Implement bulk insert completion */
}

/* Relation operations */
static void
rocks_relation_set_new_filelocator(Relation rel,
                                  const RelFileLocator *newrlocator,
                                  char persistence,
                                  TransactionId *freezeXid,
                                  MultiXactId *minmulti)
{
    Oid table_oid = RelationGetRelid(rel);
    TupleDesc tupdesc = rel->rd_att;
    int natts = tupdesc->natts;
    TableMetadata *meta;
    int i;
    
    /* Initialize RocksDB for new table */
    init_rocksdb();
    
    /* Create table metadata with column types */
    meta = get_table_metadata(table_oid, true);
    meta->column_count = natts;
    
    /* Store column types */
    for (i = 0; i < natts && i < MaxAttrNumber; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        meta->column_types[i] = attr->atttypid;
    }
    
    update_table_metadata(table_oid, meta);
    pfree(meta);
    
    /* Set transaction IDs for new table */
    if (freezeXid)
        *freezeXid = RecentXmin;
    if (minmulti)
        *minmulti = GetOldestMultiXactId();
}

static void
rocks_relation_nontransactional_truncate(Relation rel)
{
    Oid table_oid = RelationGetRelid(rel);
    char *prefix, *err = NULL;
    size_t prefix_len;
    rocksdb_iterator_t *iter;
    
    init_rocksdb();
    
    /* Create prefix for this table's data */
    prefix = make_row_prefix(table_oid, &prefix_len);
    
    /* Create iterator to find all keys for this table */
    iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    rocksdb_iter_seek(iter, prefix, prefix_len);
    
    /* Delete all keys that match the table prefix */
    while (rocksdb_iter_valid(iter)) {
        const char *key;
        size_t key_len;
        
        key = rocksdb_iter_key(iter, &key_len);
        
        /* Check if this key belongs to our table */
        if (key_len < prefix_len || 
            memcmp(key, prefix, prefix_len) != 0) {
            break;
        }
        
        /* Delete this key */
        rocksdb_delete(rocks_db, rocks_write_options, key, key_len, &err);
        if (err != NULL) {
            elog(WARNING, "Failed to delete key during truncate: %s", err);
            free(err);
            err = NULL;
        }
        
        rocksdb_iter_next(iter);
    }
    
    /* Reset the table metadata */
    {
        TableMetadata *meta = get_table_metadata(table_oid, false);
        meta->row_count = 0;
        update_table_metadata(table_oid, meta);
        pfree(meta);
    }
    
    rocksdb_iter_destroy(iter);
    pfree(prefix);
}

static void
rocks_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
    /* TODO: Implement data copying */
}

static void
rocks_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                               Relation OldIndex, bool use_sort,
                               TransactionId OldestXmin,
                               TransactionId *xid_cutoff,
                               MultiXactId *multi_cutoff,
                               double *num_tuples,
                               double *tups_vacuumed,
                               double *tups_recently_dead)
{
    /* TODO: Implement clustering copy */
}

static void
rocks_relation_vacuum(Relation onerel, VacuumParams *params,
                     BufferAccessStrategy bstrategy)
{
    /* TODO: Implement vacuum */
}

/* Analysis operations */
static bool
rocks_scan_analyze_next_block(TableScanDesc scan,
                             ReadStream *stream)
{
    elog(LOG, "rocks_scan_analyze_next_block called");
    /* TODO: Implement analysis block scanning */
    return false;
}

static bool
rocks_scan_analyze_next_tuple(TableScanDesc scan,
                             TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    elog(LOG, "rocks_scan_analyze_next_tuple called");
    /* TODO: Implement analysis tuple scanning */
    return false;
}

/* Index operations */
static double
rocks_index_build_range_scan(Relation tablerel, Relation indexrel,
                            IndexInfo *indexInfo,
                            bool allow_sync, bool anyvisible,
                            bool progress, BlockNumber start_blockno,
                            BlockNumber numblocks,
                            IndexBuildCallback callback,
                            void *callback_state,
                            TableScanDesc scan)
{
    elog(LOG, "rocks_index_build_range_scan called");
    /* TODO: Implement index building */
    return 0.0;
}

static void
rocks_index_validate_scan(Relation tablerel, Relation indexrel,
                         IndexInfo *indexInfo, Snapshot snapshot,
                         ValidateIndexState *state)
{
    elog(LOG, "rocks_index_validate_scan called");
    /* TODO: Implement index validation */
}

/* Size and estimation operations */
static uint64
rocks_relation_size(Relation rel, ForkNumber forkNumber)
{
    Oid table_oid = RelationGetRelid(rel);
    char *prefix;
    size_t prefix_len;
    rocksdb_iterator_t *iter;
    uint64 total_size = 0;
    
    /* Only handle main fork for RocksDB storage */
    if (forkNumber != MAIN_FORKNUM) {
        return 0;
    }
    
    init_rocksdb();
    
    /* Create prefix for this table's data */
    prefix = make_row_prefix(table_oid, &prefix_len);
    
    /* Create iterator to scan all keys for this table */
    iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    rocksdb_iter_seek(iter, prefix, prefix_len);
    
    /* Sum up storage size of all keys and values */
    while (rocksdb_iter_valid(iter)) {
        const char *key;
        /* const char *value; -- unused in row storage */
        size_t key_len, value_len;
        
        key = rocksdb_iter_key(iter, &key_len);
        
        /* Check if this key belongs to our table */
        if (key_len < prefix_len || 
            memcmp(key, prefix, prefix_len) != 0) {
            break;
        }
        
        rocksdb_iter_value(iter, &value_len);
        total_size += key_len + value_len;
        
        rocksdb_iter_next(iter);
    }
    
    /* Add metadata overhead */
    {
        char *meta_key;
        size_t meta_key_len;
        
        meta_key = make_metadata_key(table_oid, &meta_key_len);
        total_size += meta_key_len + sizeof(TableMetadata);
        pfree(meta_key);
    }
    
    /* Cleanup */
    rocksdb_iter_destroy(iter);
    pfree(prefix);
    
    return total_size;
}

static bool
rocks_relation_needs_toast_table(Relation rel)
{
    /* For simplicity, assume no TOAST table is needed */
    return false;
}

static void
rocks_relation_estimate_size(Relation rel, int32 *attr_widths,
                            BlockNumber *pages, double *tuples,
                            double *allvisfrac)
{
    elog(LOG, "rocks_relation_estimate_size called");
    
    /* Provide basic estimates for query planning */
    *pages = 1000;           /* Estimate 1000 pages */
    *tuples = 10000.0;       /* Estimate 10000 tuples */
    *allvisfrac = 1.0;       /* All pages are visible */
}


