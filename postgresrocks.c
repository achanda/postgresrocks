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

/* Simple global scan counter for debugging */
static int global_scan_counter = 0;

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

/* Storage format design - COLUMNAR STORAGE:
 * Key format: 
 * - Column data: "col_<oid>_<col_idx>_<chunk_id>" -> [column_values]
 * - Metadata: "meta_<oid>_info" -> [row_count, col_count, chunk_size]
 * - Row mapping: "row_<oid>_<rowid>" -> [chunk_id, chunk_offset]
 * 
 * For each table, we store:
 * - "meta_<oid>_info" -> table metadata (row count, column count, chunk size)
 * - "col_<oid>_<col_idx>_<chunk_id>" -> column data chunks
 * - "row_<oid>_<rowid>" -> row to chunk mapping
 */

/* Columnar storage constants */
#define CHUNK_SIZE 1000  /* Number of rows per chunk */

/* Column data structure */
typedef struct ColumnChunk
{
    uint32 chunk_id;
    uint32 row_count;
    char *data;
    size_t data_len;
} ColumnChunk;

/* Table metadata structure */
typedef struct TableMetadata
{
    uint64 row_count;
    uint32 column_count;
    uint32 chunk_size;
    Oid column_types[MaxAttrNumber];
} TableMetadata;

/* Row mapping structure */
typedef struct RowMapping
{
    uint32 chunk_id;
    uint32 chunk_offset;
} RowMapping;

/* Helper function to create column data key */
static char *
make_column_key(Oid table_oid, uint32 col_idx, uint32 chunk_id, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "col_%u_%u_%u", table_oid, col_idx, chunk_id);
    return key;
}

/* Helper function to create row mapping key */
static char *
make_row_mapping_key(Oid table_oid, uint64 rowid, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "row_%u_%lu", table_oid, rowid);
    return key;
}

/* Helper function to create metadata key */
static char *
make_metadata_key(Oid table_oid, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "meta_%u_info", table_oid);
    return key;
}

/* Helper function to create key prefix for table scanning (row mappings) */
static char *
make_table_prefix(Oid table_oid, size_t *prefix_len)
{
    char *prefix = palloc(32);
    *prefix_len = snprintf(prefix, 32, "row_%u_", table_oid);
    return prefix;
}

/* Custom scan descriptor for columnar storage */
typedef struct RocksScanDesc
{
    TableScanDescData rs_base;
    rocksdb_iterator_t *iterator;
    char *key_prefix;
    size_t key_prefix_len;
    uint64 current_rowid;
    bool started;
    
    /* Columnar storage specific fields */
    TableMetadata table_meta;
    uint32 current_chunk_id;
    uint32 current_chunk_offset;
    ColumnChunk *current_chunks[MaxAttrNumber];
    bool chunks_loaded;
    
    /* Scan state for table access method */
    uint64 current_row_id;
    uint64 total_rows;
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
        meta->chunk_size = CHUNK_SIZE;
        
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

/* Get next row ID and chunk information */
static void
get_next_row_info(Oid table_oid, uint64 *rowid, uint32 *chunk_id, uint32 *chunk_offset)
{
    TableMetadata *meta = get_table_metadata(table_oid, true);
    
    if (!meta) {
        elog(ERROR, "Failed to get table metadata for table %u", table_oid);
    }
    
    if (meta->chunk_size == 0) {
        meta->chunk_size = CHUNK_SIZE;
    }
    
    *rowid = meta->row_count + 1;
    
    /* Implement proper chunking with CHUNK_SIZE rows per chunk */
    *chunk_id = (*rowid - 1) / meta->chunk_size;  /* Proper chunk assignment */
    *chunk_offset = (*rowid - 1) % meta->chunk_size;  /* Offset within chunk */
    
    /* Update row count */
    meta->row_count = *rowid;
    
    /* Only update metadata every 100 inserts for better performance */
    if (meta->row_count % 100 == 0) {
        update_table_metadata(table_oid, meta);
    }
    
    pfree(meta);
}

/* Serialize a single column value to binary format */
static char *
serialize_column_value(Datum value, Oid type_oid, bool isnull, size_t *data_len)
{
    char *data;
    char *ptr;
    
    if (isnull) {
        *data_len = 0;
        return NULL;
    }
    
    switch (type_oid) {
        case INT4OID: {
            int32 val = DatumGetInt32(value);
            data = palloc(sizeof(int32));
            *((int32*)data) = val;
            *data_len = sizeof(int32);
            break;
        }
        case INT8OID: {
            int64 val = DatumGetInt64(value);
            data = palloc(sizeof(int64));
            *((int64*)data) = val;
            *data_len = sizeof(int64);
            break;
        }
        case TEXTOID:
        case VARCHAROID: {
            text *txt = DatumGetTextP(value);
            int32 len = VARSIZE(txt) - VARHDRSZ;
            data = palloc(sizeof(int32) + len);
            ptr = data;
            *((int32*)ptr) = len;
            ptr += sizeof(int32);
            memcpy(ptr, VARDATA(txt), len);
            *data_len = sizeof(int32) + len;
            break;
        }
        default:
            elog(ERROR, "Unsupported data type: %u", type_oid);
            *data_len = 0;
            return NULL;
    }
    
    return data;
}

/* Simplified serialize function for debugging - stores only single values */
static char *
serialize_column_chunk(Datum *values, bool *isnulls, Oid type_oid, int count, size_t *data_len)
{
    char *data;
    size_t total_size;
    
    /* For now, handle only single values (count=1) */
    if (count != 1) {
        elog(ERROR, "serialize_column_chunk: only single values supported, got count=%d", count);
    }
    
    if (isnulls[0]) {
        /* Null value - store just a flag */
        data = palloc(1);
        data[0] = 1; /* NULL flag */
        *data_len = 1;
        return data;
    }
    
    /* Non-null value - store type-specific data */
    switch (type_oid) {
        case INT4OID: {
            int32 val = DatumGetInt32(values[0]);
            data = palloc(1 + sizeof(int32));
            data[0] = 0; /* Not-null flag */
            memcpy(data + 1, &val, sizeof(int32));
            *data_len = 1 + sizeof(int32);
            break;
        }
        case TEXTOID:
        case VARCHAROID: {
            text *txt = DatumGetTextP(values[0]);
            int32 len = VARSIZE(txt) - VARHDRSZ;
            data = palloc(1 + sizeof(int32) + len);
            data[0] = 0; /* Not-null flag */
            memcpy(data + 1, &len, sizeof(int32));
            memcpy(data + 1 + sizeof(int32), VARDATA(txt), len);
            *data_len = 1 + sizeof(int32) + len;
            break;
        }
        default:
            elog(ERROR, "serialize_column_chunk: unsupported type %u", type_oid);
            *data_len = 0;
            return NULL;
    }
    
    return data;
}

/* Deserialize a column chunk */
static void
deserialize_column_chunk(char *data, size_t data_len, Oid type_oid, 
                        Datum *values, bool *isnulls, int count)
{
    char *ptr = data;
    uint32 stored_count;
    int bitmap_size;
    char *bitmap;
    int i;
    
    /* Read chunk header */
    stored_count = *((uint32*)ptr);
    ptr += sizeof(uint32);
    
    if (stored_count != count) {
        elog(ERROR, "Column chunk count mismatch: expected %d, got %u", count, stored_count);
    }
    
    /* Read null bitmap */
    bitmap_size = (count + 7) / 8;
    bitmap = ptr;
    ptr += bitmap_size;
    
    /* Parse null bitmap and values */
    for (i = 0; i < count; i++) {
        isnulls[i] = (bitmap[i / 8] & (1 << (i % 8))) != 0;
        
        if (!isnulls[i]) {
            switch (type_oid) {
                case INT4OID: {
                    int32 val = *((int32*)ptr);
                    values[i] = Int32GetDatum(val);
                    ptr += sizeof(int32);
                    break;
                }
                case INT8OID: {
                    int64 val = *((int64*)ptr);
                    values[i] = Int64GetDatum(val);
                    ptr += sizeof(int64);
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    int32 len = *((int32*)ptr);
                    text *txt;
                    
                    ptr += sizeof(int32);
                    txt = (text*)palloc(VARHDRSZ + len);
                    SET_VARSIZE(txt, VARHDRSZ + len);
                    memcpy(VARDATA(txt), ptr, len);
                    ptr += len;
                    
                    values[i] = PointerGetDatum(txt);
                    break;
                }
                default:
                    elog(ERROR, "Unsupported data type during deserialization: %u", type_oid);
            }
        } else {
            values[i] = (Datum)0; /* Null value */
        }
    }
}

/* Deserialize stored tuple data from speculative insertion */
static void
deserialize_stored_tuple(char *data, size_t data_len, TupleTableSlot *slot)
{
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    char *ptr = data;
    int natts, i;
    
    /* Read number of attributes */
    natts = *((int*)ptr);
    ptr += sizeof(int);
    
    ExecClearTuple(slot);
    
    for (i = 0; i < natts && i < tupdesc->natts; i++) {
        bool isnull = *((bool*)ptr);
        ptr += sizeof(bool);
        
        slot->tts_isnull[i] = isnull;
        
        if (!isnull) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            
            switch (attr->atttypid) {
                case INT4OID: {
                    int32 val = *((int32*)ptr);
                    ptr += sizeof(int32);
                    slot->tts_values[i] = Int32GetDatum(val);
                    break;
                }
                case INT8OID: {
                    int64 val = *((int64*)ptr);
                    ptr += sizeof(int64);
                    slot->tts_values[i] = Int64GetDatum(val);
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    int32 len = *((int32*)ptr);
                    text *txt;
                    
                    ptr += sizeof(int32);
                    txt = (text*)palloc(VARHDRSZ + len);
                    SET_VARSIZE(txt, VARHDRSZ + len);
                    memcpy(VARDATA(txt), ptr, len);
                    ptr += len;
                    
                    slot->tts_values[i] = PointerGetDatum(txt);
                    break;
                }
            }
        }
    }
    
    ExecStoreVirtualTuple(slot);
}

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


/* Minimal stub implementations for required functions */
static void
rocks_relation_copy_data_stub(Relation rel, const RelFileLocator *newrlocator)
{
    /* Stub implementation */
}

static void
rocks_relation_copy_for_cluster_stub(Relation OldTable, Relation NewTable,
                                     Relation OldIndex, bool use_sort,
                                     TransactionId OldestXmin,
                                     TransactionId *xid_cutoff,
                                     MultiXactId *multi_cutoff,
                                     double *num_tuples,
                                     double *tups_vacuumed,
                                     double *tups_recently_dead)
{
    /* Stub implementation */
}

static void
rocks_relation_vacuum_stub(Relation onerel, VacuumParams *params,
                          BufferAccessStrategy bstrategy)
{
    /* Stub implementation */
}

static void
rocks_finish_bulk_insert_stub(Relation relation, int options)
{
    /* Ensure metadata is updated at the end of bulk operations */
    Oid table_oid = RelationGetRelid(relation);
    TableMetadata *meta = get_table_metadata(table_oid, false);
    
    if (meta) {
        update_table_metadata(table_oid, meta);
        pfree(meta);
    }
}

static TM_Result
rocks_tuple_delete_stub(Relation relation, ItemPointer tid,
                       CommandId cid, Snapshot snapshot,
                       Snapshot crosscheck, bool wait,
                       TM_FailureData *tmfd, bool changingPart)
{
    return TM_Ok;
}

static TM_Result
rocks_tuple_update_stub(Relation relation, ItemPointer otid,
                       TupleTableSlot *slot, CommandId cid,
                       Snapshot snapshot, Snapshot crosscheck,
                       bool wait, TM_FailureData *tmfd,
                       LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    return TM_Ok;
}

static TM_Result
rocks_tuple_lock_stub(Relation relation, ItemPointer tid,
                     Snapshot snapshot, TupleTableSlot *slot,
                     CommandId cid, LockTupleMode mode,
                     LockWaitPolicy wait_policy, uint8 flags,
                     TM_FailureData *tmfd)
{
    return TM_Ok;
}

static void
rocks_multi_insert_stub(Relation relation, TupleTableSlot **slots,
                       int ntuples, CommandId cid, int options,
                       BulkInsertState bistate)
{
    /* Stub implementation */
}

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
    .tuple_insert_speculative = NULL,
    .tuple_complete_speculative = NULL,
    .multi_insert = NULL,
    .tuple_delete = NULL,
    .tuple_update = NULL,
    .tuple_lock = NULL,
    .finish_bulk_insert = NULL,

    /* DDL related functionality */
    .relation_set_new_filelocator = rocks_relation_set_new_filelocator,
    .relation_nontransactional_truncate = NULL,
    .relation_copy_data = NULL,
    .relation_copy_for_cluster = NULL,
    .relation_vacuum = NULL,
    .scan_analyze_next_block = NULL,
    .scan_analyze_next_tuple = NULL,
    .index_build_range_scan = NULL,
    .index_validate_scan = NULL,

    /* Miscellaneous functions */
    .relation_size = rocks_relation_size,
    .relation_needs_toast_table = rocks_relation_needs_toast_table,

    /* MISSING FIELDS! */
    .relation_toast_am = NULL,
    .relation_fetch_toast_slice = NULL,

    /* Planner related functions - MISSING FIELDS! */
    .relation_estimate_size = NULL,

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
        char *value;
        size_t value_len;
        char *err = NULL;
        RowMapping row_map;
        char *col_key_id, *col_key_name;
        size_t col_key_id_len, col_key_name_len;
        char *col_data_id, *col_data_name;
        size_t col_data_id_len, col_data_name_len;
        
        /* Get row mapping */
        row_key = make_row_mapping_key(table_oid, rowid, &row_key_len);
        value = rocksdb_get(rocks_db, rocks_read_options, row_key, row_key_len, &value_len, &err);
        
        if (err != NULL || value == NULL) {
            pfree(row_key);
            if (err) free(err);
            SRF_RETURN_DONE(funcctx);
        }
        
        /* Parse row mapping */
        memcpy(&row_map, value, sizeof(RowMapping));
        free(value);
        pfree(row_key);
        
        /* Get column data for ID (column 0) */
        col_key_id = make_column_key(table_oid, 0, row_map.chunk_id, &col_key_id_len);
        col_data_id = rocksdb_get(rocks_db, rocks_read_options, col_key_id, col_key_id_len, &col_data_id_len, &err);
        
        /* Get column data for name (column 1) */
        col_key_name = make_column_key(table_oid, 1, row_map.chunk_id, &col_key_name_len);
        col_data_name = rocksdb_get(rocks_db, rocks_read_options, col_key_name, col_key_name_len, &col_data_name_len, &err);
        
        /* Build a tuple with actual data */
        values = (char **) palloc(2 * sizeof(char *));
        
        /* Extract ID from column chunk using simplified format */
        if (col_data_id && col_data_id_len > 0) {
            if (col_data_id[0] == 1) {
                /* NULL value */
                values[0] = pstrdup("NULL");
            } else {
                /* Non-null integer value */
                int32 id_value;
                memcpy(&id_value, col_data_id + 1, sizeof(int32));
                values[0] = psprintf("%d", id_value);
            }
        } else {
            values[0] = pstrdup("0");
        }
        
        /* Extract name from column chunk using simplified format */
        if (col_data_name && col_data_name_len > 0) {
            if (col_data_name[0] == 1) {
                /* NULL value */
                values[1] = pstrdup("NULL");
            } else {
                /* Non-null text value */
                int32 text_len;
                char *text_data;
                
                memcpy(&text_len, col_data_name + 1, sizeof(int32));
                text_data = palloc(text_len + 1);
                memcpy(text_data, col_data_name + 1 + sizeof(int32), text_len);
                text_data[text_len] = '\0';
                values[1] = text_data;
            }
        } else {
            values[1] = pstrdup("");
        }
        
        /* Clean up */
        pfree(col_key_id);
        pfree(col_key_name);
        if (col_data_id) free(col_data_id);
        if (col_data_name) free(col_data_name);
        
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
    TableScanDescData *scan;
    
    elog(LOG, "rocks_beginscan called - creating minimal scan");
    
    /* Use standard PostgreSQL scan structure */
    scan = (TableScanDescData *) palloc0(sizeof(TableScanDescData));
    scan->rs_rd = relation;
    scan->rs_snapshot = snapshot;
    scan->rs_nkeys = nkeys;
    scan->rs_key = key;
    scan->rs_flags = flags;
    
    elog(LOG, "rocks_beginscan completed - returning scan descriptor");
    
    return (TableScanDesc) scan;
}

static void
rocks_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    elog(LOG, "rocks_rescan called");
    /* For basic scan, we don't need to do anything special */
}

/* Load column chunks for the current chunk ID */
static void
load_column_chunks(RocksScanDesc *scan, Relation relation)
{
    Oid table_oid;
    TupleDesc tupdesc;
    int natts;
    int i;
    char *key;
    size_t key_len;
    char *value, *err;
    size_t value_len;
    
    if (scan->chunks_loaded) {
        return;
    }
    
    table_oid = RelationGetRelid(relation);
    tupdesc = relation->rd_att;
    natts = tupdesc->natts;
    
    /* Clear any existing chunks */
    for (i = 0; i < MaxAttrNumber; i++) {
        if (scan->current_chunks[i]) {
            pfree(scan->current_chunks[i]->data);
            pfree(scan->current_chunks[i]);
            scan->current_chunks[i] = NULL;
        }
    }
    
    /* Load each column chunk */
    for (i = 0; i < natts; i++) {
        key = make_column_key(table_oid, i, scan->current_chunk_id, &key_len);
        err = NULL;
        
        value = rocksdb_get(rocks_db, rocks_read_options, key, key_len, &value_len, &err);
        
        if (err != NULL) {
            elog(ERROR, "RocksDB get error: %s", err);
            free(err);
        }
        
        if (value != NULL && value_len > 0) {
            scan->current_chunks[i] = palloc(sizeof(ColumnChunk));
            scan->current_chunks[i]->chunk_id = scan->current_chunk_id;
            scan->current_chunks[i]->data = value;
            scan->current_chunks[i]->data_len = value_len;
            scan->current_chunks[i]->row_count = 1; /* For now, single values */
        } else {
            scan->current_chunks[i] = NULL;
        }
        
        pfree(key);
    }
    
    scan->chunks_loaded = true;
}

/* RocksScanDesc already defined above with the fields we need */

static bool
rocks_getnextslot(TableScanDesc sscan, ScanDirection direction,
                 TupleTableSlot *slot)
{
    elog(LOG, "rocks_getnextslot called - returning false immediately");
    
    /* For debugging - return no rows to avoid any slot manipulation issues */
    return false;
}

static void
rocks_endscan(TableScanDesc sscan)
{
    elog(LOG, "rocks_endscan called");
    if (sscan) {
        /* Simple cleanup - just free the scan descriptor */
        pfree(sscan);
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
    uint32 chunk_id, chunk_offset;
    char *err = NULL;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int natts = tupdesc->natts;
    int col_idx;
    RowMapping row_map;
    char *row_key;
    size_t row_key_len;
    Form_pg_attribute attr;
    Oid type_oid;
    char *col_key;
    size_t col_key_len;
    Datum values[1];
    bool isnulls[1];
    size_t data_len;
    char *data;
    
    init_rocksdb();
    
    /* Ensure the slot is properly materialized */
    slot_getallattrs(slot);
    
    /* Get next row ID and chunk information */
    get_next_row_info(table_oid, &rowid, &chunk_id, &chunk_offset);
    
    /* Store row mapping */
    row_map.chunk_id = chunk_id;
    row_map.chunk_offset = chunk_offset;
    row_key = make_row_mapping_key(table_oid, rowid, &row_key_len);
    
    rocksdb_put(rocks_db, rocks_write_options, row_key, row_key_len,
                (char*)&row_map, sizeof(RowMapping), &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB put error for row mapping: %s", err);
        free(err);
    }
    
    /* Store column values in chunks */
    for (col_idx = 0; col_idx < natts; col_idx++) {
        elog(LOG, "Starting to process column %d", col_idx);
        attr = TupleDescAttr(tupdesc, col_idx);
        elog(LOG, "Got TupleDescAttr for column %d", col_idx);
        type_oid = attr->atttypid;
        elog(LOG, "Got type_oid %u for column %d", type_oid, col_idx);
        
        /* Create column chunk key */
        elog(LOG, "About to create column key for col %d, table_oid=%u, chunk_id=%u", col_idx, table_oid, chunk_id);
        col_key = make_column_key(table_oid, col_idx, chunk_id, &col_key_len);
        elog(LOG, "Created column key for col %d, key_len=%zu", col_idx, col_key_len);
        
        /* For now, store single values (will be optimized later) */
        elog(LOG, "About to access slot values for column %d", col_idx);
        values[0] = slot->tts_values[col_idx];
        elog(LOG, "Got slot value for column %d", col_idx);
        isnulls[0] = slot->tts_isnull[col_idx];
        elog(LOG, "Got slot isnull for column %d", col_idx);
        
        data = serialize_column_chunk(values, isnulls, type_oid, 1, &data_len);
        
        /* Debug: Log what we're storing */
        elog(LOG, "Storing column %d: data_len=%zu, type_oid=%u, value=%s, first_byte=%d", 
             col_idx, data_len, type_oid, 
             isnulls[0] ? "NULL" : (type_oid == INT4OID ? "INT" : "TEXT"),
             data_len > 0 ? (int)data[0] : -1);
        
        /* Store in RocksDB */
        elog(LOG, "About to call rocksdb_put for column %d", col_idx);
        rocksdb_put(rocks_db, rocks_write_options, col_key, col_key_len,
                    data, data_len, &err);
        elog(LOG, "rocksdb_put completed for column %d", col_idx);
        
        if (err != NULL) {
            elog(ERROR, "RocksDB put error for column data: %s", err);
            free(err);
        }
        
        elog(LOG, "About to pfree for column %d", col_idx);
        pfree(col_key);
        pfree(data);
        elog(LOG, "Completed processing column %d", col_idx);
    }
    
    pfree(row_key);
}

static void
rocks_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                              CommandId cid, int options,
                              BulkInsertState bistate,
                              uint32 specToken)
{
    Oid table_oid = RelationGetRelid(relation);
    uint64 rowid;
    uint32 chunk_id, chunk_offset;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int natts = tupdesc->natts;
    SpeculativeInsertEntry *entry;
    bool found;
    size_t total_size;
    char *ptr;
    int i;
    
    init_rocksdb();
    
    /* Get next row ID and chunk information */
    get_next_row_info(table_oid, &rowid, &chunk_id, &chunk_offset);
    
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
    entry->chunk_id = chunk_id;
    entry->chunk_offset = chunk_offset;
    
    /* Store row mapping key */
    entry->key = make_row_mapping_key(table_oid, rowid, &entry->key_len);
    
    /* Store the original tuple data for later reconstruction */
    /* Calculate size needed for tuple data */
    total_size = sizeof(int) + natts * sizeof(bool);
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
            }
        }
    }
    
    /* Allocate and serialize tuple data */
    entry->data = palloc(total_size);
    entry->data_len = total_size;
    
    ptr = entry->data;
    *((int*)ptr) = natts;
    ptr += sizeof(int);
    
    for (i = 0; i < natts; i++) {
        bool isnull = slot->tts_isnull[i];
        *((bool*)ptr) = isnull;
        ptr += sizeof(bool);
        
        if (!isnull) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            switch (attr->atttypid) {
                case INT4OID: {
                    int32 val = DatumGetInt32(slot->tts_values[i]);
                    *((int32*)ptr) = val;
                    ptr += sizeof(int32);
                    break;
                }
                case INT8OID: {
                    int64 val = DatumGetInt64(slot->tts_values[i]);
                    *((int64*)ptr) = val;
                    ptr += sizeof(int64);
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    text *txt = DatumGetTextP(slot->tts_values[i]);
                    int32 len = VARSIZE(txt) - VARHDRSZ;
                    *((int32*)ptr) = len;
                    ptr += sizeof(int32);
                    memcpy(ptr, VARDATA(txt), len);
                    ptr += len;
                    break;
                }
            }
        }
    }
    
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
    TupleTableSlot temp_slot;
    RowMapping row_map;
    TupleDesc tupdesc;
    int natts;
    int col_idx;
    Form_pg_attribute attr;
    Oid type_oid;
    char *col_key;
    size_t col_key_len;
    Datum values[1];
    bool isnulls[1];
    size_t data_len;
    char *data;
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
        /* First, reconstruct the tuple from stored data */
        temp_slot.tts_tupleDescriptor = slot->tts_tupleDescriptor;
        temp_slot.tts_values = slot->tts_values;
        temp_slot.tts_isnull = slot->tts_isnull;
        
        deserialize_stored_tuple(entry->data, entry->data_len, &temp_slot);
        
        /* Store row mapping */
        row_map.chunk_id = entry->chunk_id;
        row_map.chunk_offset = entry->chunk_offset;
        rocksdb_put(rocks_db, rocks_write_options, 
                   entry->key, entry->key_len,
                   (char*)&row_map, sizeof(RowMapping), &err);
        
        if (err != NULL) {
            elog(ERROR, "RocksDB put error during speculative completion: %s", err);
            free(err);
        }
        
        /* Store column values in chunks */
        tupdesc = slot->tts_tupleDescriptor;
        natts = tupdesc->natts;
        
        for (col_idx = 0; col_idx < natts; col_idx++) {
            attr = TupleDescAttr(tupdesc, col_idx);
            type_oid = attr->atttypid;
            
            /* Create column chunk key */
            col_key = make_column_key(entry->table_oid, col_idx, entry->chunk_id, &col_key_len);
            
            /* For now, store single values (will be optimized later) */
            values[0] = temp_slot.tts_values[col_idx];
            isnulls[0] = temp_slot.tts_isnull[col_idx];
            
            data = serialize_column_chunk(values, isnulls, type_oid, 1, &data_len);
            
            /* Store in RocksDB */
            rocksdb_put(rocks_db, rocks_write_options, col_key, col_key_len,
                        data, data_len, &err);
            
            if (err != NULL) {
                elog(ERROR, "RocksDB put error for column data: %s", err);
                free(err);
            }
            
            pfree(col_key);
            pfree(data);
        }
    } else {
        /*
         * Speculation failed - we need to "undo" the row ID allocation
         * by decrementing the row count for this table
         */
        table_oid = entry->table_oid;
        meta = get_table_metadata(table_oid, false);
        
        if (meta->row_count > 0) {
            meta->row_count--;
            update_table_metadata(table_oid, meta);
        }
        
        pfree(meta);
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
    key = make_row_mapping_key(table_oid, rowid, &key_len);
    
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
    prefix = make_table_prefix(table_oid, &prefix_len);
    
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
    prefix = make_table_prefix(table_oid, &prefix_len);
    
    /* Create iterator to scan all keys for this table */
    iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    rocksdb_iter_seek(iter, prefix, prefix_len);
    
    /* Sum up storage size of all keys and values */
    while (rocksdb_iter_valid(iter)) {
        const char *key, *value;
        size_t key_len, value_len;
        
        key = rocksdb_iter_key(iter, &key_len);
        
        /* Check if this key belongs to our table */
        if (key_len < prefix_len || 
            memcmp(key, prefix, prefix_len) != 0) {
            break;
        }
        
        value = rocksdb_iter_value(iter, &value_len);
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


