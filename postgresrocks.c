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
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"

#include <rocksdb/c.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

PG_MODULE_MAGIC;

#ifdef POSTGRESROCKS_DEBUG
#define PR_LOG(...) elog(LOG, __VA_ARGS__)
#define PR_DEBUG_ONLY(stmt) do { stmt; } while (0)
#else
#define PR_LOG(...) ((void) 0)
#define PR_DEBUG_ONLY(stmt) ((void) 0)
#endif

/* Global RocksDB instance */
static rocksdb_t* rocks_db = NULL;
static rocksdb_options_t* rocks_options = NULL;
static rocksdb_readoptions_t* rocks_read_options = NULL;
static rocksdb_writeoptions_t* rocks_write_options = NULL;
static char *postgresrocks_default_layout = "row";
static bool postgresrocks_benchmark_write_mode = false;
static char *serialize_buffer = NULL;
static size_t serialize_buffer_capacity = 0;
static uint64 postgresrocks_tuple_insert_calls = 0;
static uint64 postgresrocks_multi_insert_calls = 0;
static uint64 postgresrocks_multi_inserted_tuples = 0;
static int rocks_lock_fd = -1;
static bool rocks_lock_held = false;
typedef enum RocksSessionMode
{
    ROCKS_SESSION_NONE = 0,
    ROCKS_SESSION_READ = 1,
    ROCKS_SESSION_WRITE = 2
} RocksSessionMode;
static RocksSessionMode rocks_session_mode = ROCKS_SESSION_NONE;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#define POSTGRESROCKS_LWLOCK_TRANCHE "postgresrocks_writer_tranche"
#define POSTGRESROCKS_BGWORKER_NAME "postgresrocks_writer"
#define POSTGRESROCKS_MAX_KEY_SIZE 64
#define POSTGRESROCKS_MAX_VALUE_SIZE 8192
#define POSTGRESROCKS_ERROR_SIZE 256

typedef enum RocksWriterRequestType
{
    ROCKS_WRITER_REQ_NONE = 0,
    ROCKS_WRITER_REQ_PUT = 1,
    ROCKS_WRITER_REQ_DELETE = 2
} RocksWriterRequestType;

typedef enum RocksWriterRequestStatus
{
    ROCKS_WRITER_STATUS_IDLE = 0,
    ROCKS_WRITER_STATUS_PENDING = 1,
    ROCKS_WRITER_STATUS_DONE = 2,
    ROCKS_WRITER_STATUS_ERROR = 3
} RocksWriterRequestStatus;

typedef struct RocksWriterSharedState
{
    int writer_procno;
    int waiter_procno;
    int request_type;
    int status;
    Size key_len;
    Size value_len;
    char key[POSTGRESROCKS_MAX_KEY_SIZE];
    char value[POSTGRESROCKS_MAX_VALUE_SIZE];
    char error[POSTGRESROCKS_ERROR_SIZE];
} RocksWriterSharedState;

static RocksWriterSharedState *rocks_writer_state = NULL;
static LWLock *rocks_writer_lock = NULL;
static bool postgresrocks_writer_registered = false;

/* Hash table for tracking speculative insertions */
static HTAB* speculative_insertions = NULL;

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


/* Storage format design - ROW-BASED STORAGE:
 * Key format:
 * - Row data: [tag=1][table_oid_be][rowid_be] -> [serialized_row_data]
 * - Metadata: "meta_<oid>_info" -> [row_count, col_count]
 * 
 * For each table, we store:
 * - "meta_<oid>_info" -> table metadata (row count, column count)
 * - row key [tag=1][table_oid_be][rowid_be] -> complete serialized row data
 */

/* Row storage constants */
#define MAX_ROW_SIZE 8192  /* Maximum size for a serialized row */

typedef enum StorageLayout
{
    STORAGE_LAYOUT_ROW = 0,
    STORAGE_LAYOUT_HYBRID = 1,
    STORAGE_LAYOUT_COLUMN = 2
} StorageLayout;

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
    uint32 layout;
    Oid column_types[MaxAttrNumber];
} TableMetadata;

/* Row header structure for serialized data */
typedef struct RowHeader
{
    uint32 natts;      /* Number of attributes */
    uint32 data_len;   /* Length of row data */
} RowHeader;

#define ROW_KEY_TAG 0x01
#define ROW_KEY_LEN (1 + sizeof(uint32) + sizeof(uint64))
#define ROW_PREFIX_LEN (1 + sizeof(uint32))

static TableMetadata *get_table_metadata(Oid table_oid, bool create_if_missing);
static void update_table_metadata(Oid table_oid, TableMetadata *meta);
static void postgresrocks_xact_callback(XactEvent event, void *arg);
static void close_rocksdb_session(void);
static void init_rocksdb(RocksSessionMode requested_mode);
static void postgresrocks_shmem_request(void);
static void postgresrocks_shmem_startup(void);
void postgresrocks_writer_main(Datum main_arg);
static bool postgresrocks_writer_available(void);
static void postgresrocks_writer_put(const char *key, size_t key_len,
                                     const char *value, size_t value_len);
static void postgresrocks_writer_delete(const char *key, size_t key_len);
static void clear_row_counter_cache(void);

void _PG_init(void);

static StorageLayout
parse_storage_layout(const char *layout_name)
{
    if (layout_name == NULL || strcmp(layout_name, "row") == 0)
        return STORAGE_LAYOUT_ROW;
    if (strcmp(layout_name, "hybrid") == 0)
        return STORAGE_LAYOUT_HYBRID;
    if (strcmp(layout_name, "column") == 0)
        return STORAGE_LAYOUT_COLUMN;

    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("invalid postgresrocks layout \"%s\"", layout_name),
             errhint("Valid values are \"row\", \"hybrid\", and \"column\".")));
}

static const char *
storage_layout_name(StorageLayout layout)
{
    switch (layout)
    {
        case STORAGE_LAYOUT_ROW:
            return "row";
        case STORAGE_LAYOUT_HYBRID:
            return "hybrid";
        case STORAGE_LAYOUT_COLUMN:
            return "column";
    }

    return "row";
}

static StorageLayout
get_effective_table_layout(Relation relation)
{
    TableMetadata *meta;
    StorageLayout layout = parse_storage_layout(postgresrocks_default_layout);

    meta = get_table_metadata(RelationGetRelid(relation), false);
    if (meta != NULL)
    {
        layout = (StorageLayout) meta->layout;
        pfree(meta);
    }

    return layout;
}

static void
configure_write_options(void)
{
    if (rocks_write_options == NULL)
        return;

    rocksdb_writeoptions_set_sync(rocks_write_options, 0);
    rocksdb_writeoptions_disable_WAL(rocks_write_options,
                                     postgresrocks_benchmark_write_mode ? 1 : 0);
    rocksdb_writeoptions_set_no_slowdown(rocks_write_options,
                                         postgresrocks_benchmark_write_mode ? 1 : 0);
    rocksdb_writeoptions_set_low_pri(rocks_write_options, 0);
    rocksdb_writeoptions_set_memtable_insert_hint_per_batch(rocks_write_options,
                                                            postgresrocks_benchmark_write_mode ? 1 : 0);
}

void
_PG_init(void)
{
    if (process_shared_preload_libraries_in_progress)
    {
        BackgroundWorker worker;

        prev_shmem_request_hook = shmem_request_hook;
        shmem_request_hook = postgresrocks_shmem_request;
        prev_shmem_startup_hook = shmem_startup_hook;
        shmem_startup_hook = postgresrocks_shmem_startup;

        MemSet(&worker, 0, sizeof(worker));
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
        worker.bgw_start_time = BgWorkerStart_PostmasterStart;
        worker.bgw_restart_time = 1;
        snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgresrocks");
        snprintf(worker.bgw_function_name, BGW_MAXLEN, "postgresrocks_writer_main");
        snprintf(worker.bgw_name, BGW_MAXLEN, POSTGRESROCKS_BGWORKER_NAME);
        snprintf(worker.bgw_type, BGW_MAXLEN, POSTGRESROCKS_BGWORKER_NAME);
        worker.bgw_notify_pid = 0;

        RegisterBackgroundWorker(&worker);
        postgresrocks_writer_registered = true;
    }

    DefineCustomStringVariable("postgresrocks.default_layout",
                               "Default storage layout hint for new postgresrocks tables.",
                               "Accepted values are row, hybrid, and column.",
                               &postgresrocks_default_layout,
                               "row",
                               PGC_USERSET,
                               0,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomBoolVariable("postgresrocks.benchmark_write_mode",
                             "Use faster but less durable RocksDB write settings.",
                             "When enabled, WAL is disabled for RocksDB writes and batch hints are enabled.",
                             &postgresrocks_benchmark_write_mode,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    RegisterXactCallback(postgresrocks_xact_callback, NULL);
}

static void
postgresrocks_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(MAXALIGN(sizeof(RocksWriterSharedState)));
    RequestNamedLWLockTranche(POSTGRESROCKS_LWLOCK_TRANCHE, 1);
}

static void
postgresrocks_shmem_startup(void)
{
    bool found;
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    rocks_writer_state = ShmemInitStruct("postgresrocks writer state",
                                         sizeof(RocksWriterSharedState),
                                         &found);
    if (!found)
    {
        MemSet(rocks_writer_state, 0, sizeof(RocksWriterSharedState));
        rocks_writer_state->writer_procno = -1;
        rocks_writer_state->waiter_procno = -1;
    }

    rocks_writer_lock = &(GetNamedLWLockTranche(POSTGRESROCKS_LWLOCK_TRANCHE))[0].lock;
    LWLockRelease(AddinShmemInitLock);
}

/* Helper function to create row data key */
static inline void
encode_u32_be(unsigned char *dest, uint32 value)
{
    dest[0] = (unsigned char) ((value >> 24) & 0xFF);
    dest[1] = (unsigned char) ((value >> 16) & 0xFF);
    dest[2] = (unsigned char) ((value >> 8) & 0xFF);
    dest[3] = (unsigned char) (value & 0xFF);
}

static inline void
encode_u64_be(unsigned char *dest, uint64 value)
{
    dest[0] = (unsigned char) ((value >> 56) & 0xFF);
    dest[1] = (unsigned char) ((value >> 48) & 0xFF);
    dest[2] = (unsigned char) ((value >> 40) & 0xFF);
    dest[3] = (unsigned char) ((value >> 32) & 0xFF);
    dest[4] = (unsigned char) ((value >> 24) & 0xFF);
    dest[5] = (unsigned char) ((value >> 16) & 0xFF);
    dest[6] = (unsigned char) ((value >> 8) & 0xFF);
    dest[7] = (unsigned char) (value & 0xFF);
}

static char *
make_row_key(Oid table_oid, uint64 rowid, size_t *key_len)
{
    unsigned char *key = (unsigned char *) palloc(ROW_KEY_LEN);

    key[0] = ROW_KEY_TAG;
    encode_u32_be(key + 1, (uint32) table_oid);
    encode_u64_be(key + 1 + sizeof(uint32), rowid);
    *key_len = ROW_KEY_LEN;

    return (char *) key;
}

static inline void
fill_row_key(unsigned char *key, Oid table_oid, uint64 rowid)
{
    key[0] = ROW_KEY_TAG;
    encode_u32_be(key + 1, (uint32) table_oid);
    encode_u64_be(key + 1 + sizeof(uint32), rowid);
}

/* Helper function to create key prefix for table scanning */
static char *
make_row_prefix(Oid table_oid, size_t *prefix_len)
{
    unsigned char *prefix = (unsigned char *) palloc(ROW_PREFIX_LEN);

    prefix[0] = ROW_KEY_TAG;
    encode_u32_be(prefix + 1, (uint32) table_oid);
    *prefix_len = ROW_PREFIX_LEN;

    return (char *) prefix;
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

static char *
serialize_row_reuse(TupleTableSlot *slot, size_t *data_len)
{
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int natts = tupdesc->natts;
    char *data, *ptr;
    size_t total_size;
    int i;
    RowHeader header;

    total_size = sizeof(RowHeader) + natts * sizeof(bool);

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

    if (serialize_buffer_capacity < total_size) {
        size_t new_capacity = Max(total_size, serialize_buffer_capacity == 0 ? 1024 : serialize_buffer_capacity * 2);

        if (serialize_buffer == NULL)
            serialize_buffer = MemoryContextAlloc(TopMemoryContext, new_capacity);
        else
            serialize_buffer = repalloc(serialize_buffer, new_capacity);

        serialize_buffer_capacity = new_capacity;
    }

    data = serialize_buffer;
    ptr = data;

    header.natts = natts;
    header.data_len = total_size;
    memcpy(ptr, &header, sizeof(RowHeader));
    ptr += sizeof(RowHeader);

    for (i = 0; i < natts; i++) {
        *((bool*)ptr) = slot->tts_isnull[i];
        ptr += sizeof(bool);
    }

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
    rocksdb_iterator_t *iter;
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

    
/* Function to initialize RocksDB */
static void
init_rocksdb(RocksSessionMode requested_mode)
{
    char *err = NULL;
    int lock_mode;

    if (requested_mode == ROCKS_SESSION_NONE)
        requested_mode = ROCKS_SESSION_READ;

    if (rocks_db != NULL)
    {
        if (rocks_session_mode >= requested_mode)
            return;

        /* Upgrade read session to write session by reopening under exclusive lock. */
        close_rocksdb_session();
    }

    if (rocks_lock_fd == -1)
    {
        rocks_lock_fd = open("postgresrocks_data.pglock", O_RDWR | O_CREAT, 0600);
        if (rocks_lock_fd < 0)
            elog(ERROR, "Failed to open postgresrocks coordination lock file: %m");
    }

    if (!rocks_lock_held)
    {
        lock_mode = (requested_mode == ROCKS_SESSION_WRITE) ? LOCK_EX : LOCK_SH;
        if (flock(rocks_lock_fd, lock_mode) != 0)
            elog(ERROR, "Failed to acquire postgresrocks coordination lock: %m");
        rocks_lock_held = true;
    }

    if (rocks_options == NULL)
    {
        rocks_options = rocksdb_options_create();
        rocksdb_options_set_create_if_missing(rocks_options, 1);
        rocksdb_options_set_compression(rocks_options, rocksdb_no_compression);
    }

    if (rocks_read_options == NULL)
        rocks_read_options = rocksdb_readoptions_create();

    if (rocks_write_options == NULL)
        rocks_write_options = rocksdb_writeoptions_create();

    configure_write_options();

    /* Open database while holding the coordination lock for this transaction. */
    if (requested_mode == ROCKS_SESSION_WRITE)
    {
        rocks_db = rocksdb_open(rocks_options, "postgresrocks_data", &err);
    }
    else
    {
        rocks_db = rocksdb_open_for_read_only(rocks_options, "postgresrocks_data", 0, &err);
    }

    if (err != NULL) {
        if (rocks_lock_held)
        {
            flock(rocks_lock_fd, LOCK_UN);
            rocks_lock_held = false;
        }
        rocks_session_mode = ROCKS_SESSION_NONE;
        elog(ERROR, "Failed to open RocksDB: %s", err);
        free(err);
    }

    rocks_session_mode = requested_mode;

    /* Initialize speculative insertions tracking */
    init_speculative_hash();
}

static void
close_rocksdb_session(void)
{
    if (rocks_db != NULL)
    {
        rocksdb_close(rocks_db);
        rocks_db = NULL;
    }

    if (rocks_lock_held)
    {
        if (flock(rocks_lock_fd, LOCK_UN) != 0)
            elog(WARNING, "Failed to release postgresrocks coordination lock: %m");
        rocks_lock_held = false;
    }

    rocks_session_mode = ROCKS_SESSION_NONE;
}

static bool
postgresrocks_writer_available(void)
{
    return rocks_writer_state != NULL &&
           rocks_writer_lock != NULL &&
           rocks_writer_state->writer_procno >= 0;
}

static void
postgresrocks_writer_submit_request(RocksWriterRequestType request_type,
                                    const char *key, size_t key_len,
                                    const char *value, size_t value_len)
{
    int worker_procno;

    if (!postgresrocks_writer_available())
        elog(ERROR, "postgresrocks writer is not available; preload the extension via shared_preload_libraries");

    if (key_len > POSTGRESROCKS_MAX_KEY_SIZE || value_len > POSTGRESROCKS_MAX_VALUE_SIZE)
        elog(ERROR, "postgresrocks writer request is too large");

    for (;;)
    {
        LWLockAcquire(rocks_writer_lock, LW_EXCLUSIVE);
        if (rocks_writer_state->status == ROCKS_WRITER_STATUS_IDLE ||
            rocks_writer_state->status == ROCKS_WRITER_STATUS_DONE ||
            rocks_writer_state->status == ROCKS_WRITER_STATUS_ERROR)
        {
            rocks_writer_state->request_type = request_type;
            rocks_writer_state->status = ROCKS_WRITER_STATUS_PENDING;
            rocks_writer_state->waiter_procno = MyProcNumber;
            rocks_writer_state->key_len = key_len;
            rocks_writer_state->value_len = value_len;
            memcpy(rocks_writer_state->key, key, key_len);
            if (value_len > 0 && value != NULL)
                memcpy(rocks_writer_state->value, value, value_len);
            rocks_writer_state->error[0] = '\0';
            worker_procno = rocks_writer_state->writer_procno;
            LWLockRelease(rocks_writer_lock);
            SetLatch(&ProcGlobal->allProcs[worker_procno].procLatch);
            break;
        }
        LWLockRelease(rocks_writer_lock);
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         10L,
                         0);
        ResetLatch(MyLatch);
    }

    for (;;)
    {
        LWLockAcquire(rocks_writer_lock, LW_EXCLUSIVE);
        if (rocks_writer_state->status == ROCKS_WRITER_STATUS_DONE)
        {
            rocks_writer_state->status = ROCKS_WRITER_STATUS_IDLE;
            LWLockRelease(rocks_writer_lock);
            break;
        }
        if (rocks_writer_state->status == ROCKS_WRITER_STATUS_ERROR)
        {
            char errbuf[POSTGRESROCKS_ERROR_SIZE];

            strlcpy(errbuf, rocks_writer_state->error, sizeof(errbuf));
            rocks_writer_state->status = ROCKS_WRITER_STATUS_IDLE;
            LWLockRelease(rocks_writer_lock);
            elog(ERROR, "postgresrocks writer request failed: %s", errbuf);
        }
        LWLockRelease(rocks_writer_lock);
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         10L,
                         0);
        ResetLatch(MyLatch);
    }
}

static void
postgresrocks_writer_put(const char *key, size_t key_len,
                         const char *value, size_t value_len)
{
    postgresrocks_writer_submit_request(ROCKS_WRITER_REQ_PUT,
                                        key, key_len, value, value_len);
}

static void
postgresrocks_writer_delete(const char *key, size_t key_len)
{
    postgresrocks_writer_submit_request(ROCKS_WRITER_REQ_DELETE,
                                        key, key_len, NULL, 0);
}

void
postgresrocks_writer_main(Datum main_arg)
{
    char *err = NULL;
    rocksdb_t *writer_db;
    rocksdb_options_t *writer_options;
    rocksdb_writeoptions_t *writer_write_options;

    BackgroundWorkerUnblockSignals();

    writer_options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(writer_options, 1);
    rocksdb_options_set_compression(writer_options, rocksdb_no_compression);
    writer_write_options = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(writer_write_options, 0);
    rocksdb_writeoptions_disable_WAL(writer_write_options, 0);

    writer_db = rocksdb_open(writer_options, "postgresrocks_data", &err);
    if (err != NULL)
        ereport(FATAL, (errmsg("postgresrocks writer failed to open RocksDB: %s", err)));

    LWLockAcquire(rocks_writer_lock, LW_EXCLUSIVE);
    rocks_writer_state->writer_procno = MyProcNumber;
    rocks_writer_state->status = ROCKS_WRITER_STATUS_IDLE;
    rocks_writer_state->waiter_procno = -1;
    LWLockRelease(rocks_writer_lock);

    for (;;)
    {
        int rc;
        RocksWriterRequestType request_type = ROCKS_WRITER_REQ_NONE;
        int waiter_procno = -1;
        Size key_len = 0;
        Size value_len = 0;
        char key[POSTGRESROCKS_MAX_KEY_SIZE];
        char value[POSTGRESROCKS_MAX_VALUE_SIZE];
        char local_err[POSTGRESROCKS_ERROR_SIZE];

        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       1000L,
                       0);
        ResetLatch(MyLatch);

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        LWLockAcquire(rocks_writer_lock, LW_EXCLUSIVE);
        if (rocks_writer_state->status == ROCKS_WRITER_STATUS_PENDING)
        {
            request_type = (RocksWriterRequestType) rocks_writer_state->request_type;
            waiter_procno = rocks_writer_state->waiter_procno;
            key_len = rocks_writer_state->key_len;
            value_len = rocks_writer_state->value_len;
            memcpy(key, rocks_writer_state->key, key_len);
            if (value_len > 0)
                memcpy(value, rocks_writer_state->value, value_len);
        }
        LWLockRelease(rocks_writer_lock);

        if (request_type == ROCKS_WRITER_REQ_NONE)
            continue;

        local_err[0] = '\0';
        if (request_type == ROCKS_WRITER_REQ_PUT)
            rocksdb_put(writer_db, writer_write_options, key, key_len, value, value_len, &err);
        else if (request_type == ROCKS_WRITER_REQ_DELETE)
            rocksdb_delete(writer_db, writer_write_options, key, key_len, &err);

        LWLockAcquire(rocks_writer_lock, LW_EXCLUSIVE);
        if (err != NULL)
        {
            strlcpy(local_err, err, sizeof(local_err));
            rocks_writer_state->status = ROCKS_WRITER_STATUS_ERROR;
            strlcpy(rocks_writer_state->error, local_err, sizeof(rocks_writer_state->error));
            free(err);
            err = NULL;
        }
        else
        {
            rocks_writer_state->status = ROCKS_WRITER_STATUS_DONE;
            rocks_writer_state->error[0] = '\0';
        }
        LWLockRelease(rocks_writer_lock);

        if (waiter_procno >= 0)
            SetLatch(&ProcGlobal->allProcs[waiter_procno].procLatch);
    }
}

/* Get or create table metadata */
static TableMetadata*
get_table_metadata(Oid table_oid, bool create_if_missing)
{
    char *key, *value, *err = NULL;
    size_t key_len, value_len;
    TableMetadata *meta = palloc0(sizeof(TableMetadata));
    
    init_rocksdb(ROCKS_SESSION_READ);
    
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
        
        update_table_metadata(table_oid, meta);
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

    if (postgresrocks_writer_available())
    {
        postgresrocks_writer_put(key, key_len, (char *) meta, sizeof(TableMetadata));
    }
    else
    {
        init_rocksdb(ROCKS_SESSION_WRITE);

        rocksdb_put(rocks_db, rocks_write_options, key, key_len,
                    (char*)meta, sizeof(TableMetadata), &err);

        if (err != NULL) {
            elog(ERROR, "RocksDB put error: %s", err);
            free(err);
        }
    }
    
    pfree(key);
}

/* Global row counter for batch operations */
static HTAB* row_counters = NULL;

/* Global write batch for accumulating single inserts */
static rocksdb_writebatch_t* global_batch = NULL;
static int batch_size = 0;
#define MAX_BATCH_SIZE 10000

typedef struct RowCounterEntry
{
    Oid table_oid;     /* Hash key */
    uint64 next_rowid; /* Next row ID to assign */
    bool dirty;        /* Whether metadata needs updating */
} RowCounterEntry;

/* Initialize row counter hash table */
static void
init_row_counter_hash(void)
{
    HASHCTL info;
    
    if (row_counters != NULL)
        return; /* Already initialized */
    
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(RowCounterEntry);
    info.hcxt = TopMemoryContext;
    
    row_counters = hash_create("Row Counters",
                              256, /* initial size */
                              &info,
                              HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Get next row ID for insertion (optimized for bulk operations) */
static uint64
get_next_row_id(Oid table_oid)
{
    RowCounterEntry *entry;
    bool found;
    TableMetadata *meta;
    uint64 rowid;
    
    init_row_counter_hash();
    
    /* Look up cached counter */
    entry = (RowCounterEntry *) hash_search(row_counters,
                                           &table_oid,
                                           HASH_ENTER,
                                           &found);
    
    if (!found) {
        /* Initialize from metadata */
        meta = get_table_metadata(table_oid, true);
        if (!meta) {
            elog(ERROR, "Failed to get table metadata for table %u", table_oid);
        }
        
        entry->table_oid = table_oid;
        entry->next_rowid = meta->row_count + 1;
        entry->dirty = false;
        PR_LOG("Initialized row counter: table_oid=%u, next_rowid=%llu, row_count=%llu",
             table_oid, (unsigned long long) entry->next_rowid,
             (unsigned long long) meta->row_count);
        pfree(meta);
    } else {
        PR_LOG("Found existing row counter: table_oid=%u, next_rowid=%llu, dirty=%s",
             table_oid, (unsigned long long) entry->next_rowid,
             entry->dirty ? "true" : "false");
    }
    
    rowid = entry->next_rowid++;
    entry->dirty = true;
    
    PR_LOG("Assigned row ID %llu for table %u (next will be %llu)",
         (unsigned long long) rowid, table_oid,
         (unsigned long long) entry->next_rowid);
    return rowid;
}

/* Flush pending write batch */
static void
clear_row_counter_cache(void)
{
    HASH_SEQ_STATUS status;
    RowCounterEntry *entry;

    if (row_counters == NULL)
        return;

    hash_seq_init(&status, row_counters);
    while ((entry = hash_seq_search(&status)) != NULL)
        hash_search(row_counters, &entry->table_oid, HASH_REMOVE, NULL);
}

static void
flush_write_batch(void)
{
    char *err = NULL;

    if (global_batch == NULL || batch_size == 0) {
        PR_LOG("flush_write_batch called but no batch to flush");
        return;
    }

    if (postgresrocks_writer_available())
        elog(ERROR, "write batching is not supported when the persistent postgresrocks writer is enabled");

    init_rocksdb(ROCKS_SESSION_WRITE);
    PR_LOG("About to flush write batch with %d operations", batch_size);
    
    /* Execute the accumulated batch */
    rocksdb_write(rocks_db, rocks_write_options, global_batch, &err);
    
    if (err != NULL) {
        elog(ERROR, "RocksDB batch write error: %s", err);
        free(err);
        return;
    }
    
    PR_LOG("Successfully flushed write batch with %d accumulated operations", batch_size);
    
    /* Reset batch */
    rocksdb_writebatch_clear(global_batch);
    batch_size = 0;
}

static void flush_row_counter_metadata(Oid table_oid);

/* Force flush of any pending writes - called from scan operations */
static void
ensure_writes_flushed(void)
{
    if (global_batch != NULL && batch_size > 0) {
        PR_LOG("Forcing flush of pending writes before scan operation");
        flush_write_batch();
    }
    
    /* Also ensure any dirty row counters are flushed to metadata */
    if (row_counters != NULL) {
        HASH_SEQ_STATUS status;
        RowCounterEntry *entry;
        
        hash_seq_init(&status, row_counters);
        while ((entry = hash_seq_search(&status)) != NULL) {
            if (entry->dirty) {
                PR_LOG("Flushing dirty row counter for table_oid=%u before scan", entry->table_oid);
                flush_row_counter_metadata(entry->table_oid);
            }
        }
    }
}

/* Flush row counter metadata to storage */
static void
flush_row_counter_metadata(Oid table_oid)
{
    RowCounterEntry *entry;
    bool found;
    TableMetadata *meta;
    
    PR_LOG("flush_row_counter_metadata called for table_oid=%u", table_oid);
    
    /* First flush any pending writes */
    flush_write_batch();
    
    if (row_counters == NULL) {
        PR_LOG("Row counters hash table is NULL");
        return;
    }
    
    entry = (RowCounterEntry *) hash_search(row_counters,
                                           &table_oid,
                                           HASH_FIND,
                                           &found);
    
    if (!found) {
        PR_LOG("No row counter entry found for table_oid=%u", table_oid);
        return;
    }
    
    if (!entry->dirty) {
        PR_LOG("Row counter for table_oid=%u is not dirty (next_rowid=%llu)",
             table_oid, (unsigned long long) entry->next_rowid);
        return;
    }
    
    PR_LOG("Found dirty row counter: table_oid=%u, next_rowid=%llu, will set row_count=%llu",
         table_oid, (unsigned long long) entry->next_rowid,
         (unsigned long long) (entry->next_rowid - 1));
    
    meta = get_table_metadata(table_oid, true);
    if (meta) {
        meta->row_count = entry->next_rowid - 1; /* -1 because next_rowid is the next to assign */
        update_table_metadata(table_oid, meta);
        pfree(meta);
        entry->dirty = false;
        PR_LOG("Successfully flushed metadata: table_oid=%u, row_count=%llu",
             table_oid, (unsigned long long) meta->row_count);
    } else {
        elog(ERROR, "Failed to get table metadata for flushing");
    }
}

static void
reset_dirty_row_counters(void)
{
    HASH_SEQ_STATUS status;
    RowCounterEntry *entry;

    if (row_counters == NULL)
        return;

    hash_seq_init(&status, row_counters);
    while ((entry = hash_seq_search(&status)) != NULL) {
        if (entry->dirty) {
            TableMetadata *meta = get_table_metadata(entry->table_oid, false);

            if (meta != NULL) {
                entry->next_rowid = meta->row_count + 1;
                pfree(meta);
            } else {
                entry->next_rowid = 1;
            }
            entry->dirty = false;
        }
    }
}

static void
postgresrocks_xact_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
        case XACT_EVENT_PARALLEL_PRE_COMMIT:
            if (rocks_db != NULL) {
                HASH_SEQ_STATUS status;
                RowCounterEntry *entry;

                flush_write_batch();

                if (row_counters != NULL) {
                    hash_seq_init(&status, row_counters);
                    while ((entry = hash_seq_search(&status)) != NULL) {
                        if (entry->dirty)
                            flush_row_counter_metadata(entry->table_oid);
                    }
                }

                clear_row_counter_cache();
                close_rocksdb_session();
            }
            break;

        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            if (global_batch != NULL)
                rocksdb_writebatch_clear(global_batch);
            batch_size = 0;
            reset_dirty_row_counters();
            clear_row_counter_cache();
            close_rocksdb_session();
            break;

        case XACT_EVENT_COMMIT:
        case XACT_EVENT_PARALLEL_COMMIT:
            clear_row_counter_cache();
            close_rocksdb_session();
            break;

        default:
            break;
    }
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
static void rocks_relation_estimate_size(Relation rel, int32 *attr_widths,
                                        BlockNumber *pages, double *tuples,
                                        double *allvisfrac);


static const TableAmRoutine rocks_methods = {
    .type = T_TableAmRoutine,

    /* Slot related callbacks */
    .slot_callbacks = rocks_slot_callbacks,

    /* Table scan callbacks */
    .scan_begin = rocks_beginscan,
    .scan_end = rocks_endscan,
    .scan_rescan = rocks_rescan,
    .scan_getnextslot = rocks_getnextslot,

    /* TID range scan callbacks */
    .scan_set_tidrange = NULL,
    .scan_getnextslot_tidrange = NULL,

    /* Parallel table scan callbacks */
    .parallelscan_estimate = NULL,
    .parallelscan_initialize = NULL,
    .parallelscan_reinitialize = NULL,

    /* Index scan callbacks */
    .index_fetch_begin = NULL,
    .index_fetch_reset = NULL,
    .index_fetch_end = NULL,
    .index_fetch_tuple = NULL,

    /* Non-modifying operations on individual tuples */
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

    .relation_toast_am = NULL,
    .relation_fetch_toast_slice = NULL,

    /* Planner related functions */
    .relation_estimate_size = rocks_relation_estimate_size,

    /* Executor related functions */
    .scan_bitmap_next_tuple = NULL,
    .scan_sample_next_block = NULL,
    .scan_sample_next_tuple = NULL,
};

/* Entry point function */
PG_FUNCTION_INFO_V1(postgresrocks_tableam_handler);
Datum
postgresrocks_tableam_handler(PG_FUNCTION_ARGS)
{
    PR_LOG("postgresrocks_tableam_handler called - returning TableAmRoutine");
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
        init_rocksdb(ROCKS_SESSION_READ);
        
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

PG_FUNCTION_INFO_V1(postgresrocks_set_table_layout);
Datum
postgresrocks_set_table_layout(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);
    text *layout_text = PG_GETARG_TEXT_P(1);
    char *layout_name = text_to_cstring(layout_text);
    StorageLayout layout = parse_storage_layout(layout_name);
    TableMetadata *meta;

    init_rocksdb(ROCKS_SESSION_WRITE);

    meta = get_table_metadata(table_oid, true);
    meta->layout = (uint32) layout;
    update_table_metadata(table_oid, meta);
    pfree(meta);

    PG_RETURN_TEXT_P(cstring_to_text(storage_layout_name(layout)));
}

PG_FUNCTION_INFO_V1(postgresrocks_get_table_layout);
Datum
postgresrocks_get_table_layout(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);
    TableMetadata *meta;
    StorageLayout layout = parse_storage_layout(postgresrocks_default_layout);

    init_rocksdb(ROCKS_SESSION_READ);

    meta = get_table_metadata(table_oid, false);
    if (meta != NULL)
    {
        layout = (StorageLayout) meta->layout;
        pfree(meta);
    }

    PG_RETURN_TEXT_P(cstring_to_text(storage_layout_name(layout)));
}

PG_FUNCTION_INFO_V1(postgresrocks_reset_insert_stats);
Datum
postgresrocks_reset_insert_stats(PG_FUNCTION_ARGS)
{
    postgresrocks_tuple_insert_calls = 0;
    postgresrocks_multi_insert_calls = 0;
    postgresrocks_multi_inserted_tuples = 0;

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(postgresrocks_insert_stats);
Datum
postgresrocks_insert_stats(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[3];
    bool nulls[3] = {false, false, false};

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    values[0] = Int64GetDatum((int64) postgresrocks_tuple_insert_calls);
    values[1] = Int64GetDatum((int64) postgresrocks_multi_insert_calls);
    values[2] = Int64GetDatum((int64) postgresrocks_multi_inserted_tuples);

    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(postgresrocks_force_flush);
Datum
postgresrocks_force_flush(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS status;
    RowCounterEntry *entry;

    if (rocks_db != NULL) {
        flush_write_batch();

        if (row_counters != NULL) {
            hash_seq_init(&status, row_counters);
            while ((entry = hash_seq_search(&status)) != NULL) {
                if (entry->dirty)
                    flush_row_counter_metadata(entry->table_oid);
            }
        }
    }

    PG_RETURN_VOID();
}

/* Slot callbacks - use heap tuple table slots for compatibility */
static const TupleTableSlotOps *
rocks_slot_callbacks(Relation relation)
{
    PR_LOG("rocks_slot_callbacks returning TTSOpsVirtual");
    return &TTSOpsVirtual;
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
    
    PR_LOG("rocks_beginscan called - creating scan with iterator");
    
    /* Ensure any pending writes are flushed before scanning */
    ensure_writes_flushed();
    
    init_rocksdb(ROCKS_SESSION_READ);
    
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
    scan->iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    scan->current_rowid = 1;  /* Start from first row */
    scan->started = false;
    PR_DEBUG_ONLY(
        StorageLayout layout = get_effective_table_layout(relation);
        if (layout != STORAGE_LAYOUT_ROW)
            PR_LOG("layout hint for relation %s is \"%s\"; using row-store scan path until alternative layouts are implemented",
                   RelationGetRelationName(relation), storage_layout_name(layout));
    );
    
    /* Get table metadata to know how many rows we have */
    meta = get_table_metadata(table_oid, false);
    if (meta) {
        scan->total_rows = meta->row_count;
        PR_LOG("rocks_beginscan - found metadata: table_oid=%u, row_count=%llu",
             table_oid, (unsigned long long) meta->row_count);
        pfree(meta);
    } else {
        scan->total_rows = 0;
        PR_LOG("rocks_beginscan - NO metadata found for table_oid=%u", table_oid);
    }
    
    PR_LOG("rocks_beginscan completed - scan ready, total_rows=%llu",
         (unsigned long long) scan->total_rows);
    
    return (TableScanDesc) scan;
}

static void
rocks_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
            bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    PR_LOG("rocks_rescan called");
}

static bool
rocks_getnextslot(TableScanDesc sscan, ScanDirection direction,
                 TupleTableSlot *slot)
{
    RocksScanDesc *scan = (RocksScanDesc *) sscan;
    char *row_data;
    const char *key;
    size_t key_len;
    size_t row_data_len;
    
    PR_LOG("rocks_getnextslot called - current_rowid=%llu, total_rows=%llu",
         (unsigned long long) scan->current_rowid,
         (unsigned long long) scan->total_rows);
    
    if (!scan->started) {
        rocksdb_iter_seek(scan->iter, scan->key_prefix, scan->key_prefix_len);
        scan->started = true;
    }

    if (!rocksdb_iter_valid(scan->iter)) {
        PR_LOG("rocks_getnextslot - iterator exhausted, returning false");
        return false;
    }

    key = rocksdb_iter_key(scan->iter, &key_len);
    if (key_len < scan->key_prefix_len ||
        memcmp(key, scan->key_prefix, scan->key_prefix_len) != 0) {
        PR_LOG("rocks_getnextslot - prefix no longer matches, returning false");
        return false;
    }

    row_data = (char *) rocksdb_iter_value(scan->iter, &row_data_len);

    PR_LOG("rocks_getnextslot - found row %llu, data_len=%zu",
         (unsigned long long) scan->current_rowid, row_data_len);
    
    /* Deserialize the complete row into the slot */
    deserialize_row(row_data, row_data_len, slot);
    
    /* Move to next row */
    scan->current_rowid++;
    rocksdb_iter_next(scan->iter);
    
    PR_LOG("rocks_getnextslot - successfully deserialized row %llu",
         (unsigned long long) (scan->current_rowid - 1));
    return true;
}

static void
rocks_endscan(TableScanDesc sscan)
{
    RocksScanDesc *scan = (RocksScanDesc *) sscan;
    
    PR_LOG("rocks_endscan called");
    if (scan) {
        /* Cleanup scan state */
        if (scan->iter) {
            rocksdb_iter_destroy(scan->iter);
        }
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
    unsigned char row_key[ROW_KEY_LEN];
    char *row_data;
    size_t row_data_len;
    
    PR_LOG("rocks_tuple_insert - table_oid=%u", table_oid);
    postgresrocks_tuple_insert_calls++;
    
    /* Ensure the slot is properly materialized */
    slot_getallattrs(slot);
    PR_DEBUG_ONLY(
        StorageLayout layout = get_effective_table_layout(relation);
        if (layout != STORAGE_LAYOUT_ROW)
            PR_LOG("layout hint for relation %s is \"%s\"; using row-store insert path until alternative layouts are implemented",
                   RelationGetRelationName(relation), storage_layout_name(layout));
    );
    
    /* Get next row ID (cached for performance) */
    rowid = get_next_row_id(table_oid);
    
    /* Serialize the complete row */
    row_data = serialize_row_reuse(slot, &row_data_len);
    
    /* Create row key */
    fill_row_key(row_key, table_oid, rowid);
    
    if (postgresrocks_writer_available())
    {
        postgresrocks_writer_put((const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
    }
    else
    {
        init_rocksdb(ROCKS_SESSION_WRITE);

        /* Initialize global batch if needed */
        if (global_batch == NULL) {
            global_batch = rocksdb_writebatch_create();
            batch_size = 0;
        }

        /* Add to batch instead of immediate write */
        rocksdb_writebatch_put(global_batch, (const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
        batch_size++;

        /* Flush batch if it's getting large */
        if (batch_size >= MAX_BATCH_SIZE) {
            flush_write_batch();
        }
    }
    
    PR_LOG("rocks_tuple_insert - added row %llu to batch (size=%d), data_len=%zu",
         (unsigned long long) rowid, batch_size, row_data_len);
    
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
        if (postgresrocks_writer_available())
        {
            postgresrocks_writer_put(entry->key, entry->key_len, entry->data, entry->data_len);
        }
        else
        {
            init_rocksdb(ROCKS_SESSION_WRITE);
            rocksdb_put(rocks_db, rocks_write_options,
                       entry->key, entry->key_len,
                       entry->data, entry->data_len, &err);

            if (err != NULL) {
                elog(ERROR, "RocksDB put error during speculative completion: %s", err);
                free(err);
            }
        }
        
        PR_LOG("Speculative insertion succeeded for row %llu",
             (unsigned long long) entry->rowid);
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
        
        PR_LOG("Speculative insertion failed for row %llu",
             (unsigned long long) entry->rowid);
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
    Oid table_oid = RelationGetRelid(relation);
    rocksdb_writebatch_t* batch;
    char *err = NULL;
    int i;
    
    PR_LOG("rocks_multi_insert - inserting %d tuples for table_oid=%u", ntuples, table_oid);
    postgresrocks_multi_insert_calls++;
    postgresrocks_multi_inserted_tuples += ntuples;
    
    if (!postgresrocks_writer_available())
    {
        init_rocksdb(ROCKS_SESSION_WRITE);
        /* Create a write batch for optimal performance */
        batch = rocksdb_writebatch_create();
    }
    
    for (i = 0; i < ntuples; i++) {
        uint64 rowid;
        unsigned char row_key[ROW_KEY_LEN];
        char *row_data;
        size_t row_data_len;
        
        /* Ensure the slot is properly materialized */
        slot_getallattrs(slots[i]);
        
        /* Get next row ID (cached, no metadata write yet) */
        rowid = get_next_row_id(table_oid);
        
        /* Serialize the complete row */
        row_data = serialize_row_reuse(slots[i], &row_data_len);
        
        /* Create row key */
        fill_row_key(row_key, table_oid, rowid);
        
        if (postgresrocks_writer_available())
            postgresrocks_writer_put((const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
        else
            rocksdb_writebatch_put(batch, (const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
    }
    
    /* Execute the entire batch in one operation */
    if (!postgresrocks_writer_available())
    {
        rocksdb_write(rocks_db, rocks_write_options, batch, &err);

        if (err != NULL) {
            elog(ERROR, "RocksDB batch write error: %s", err);
            free(err);
        }
    }
    
    /* Flush metadata once at the end */
    flush_row_counter_metadata(table_oid);
    
    /* Cleanup */
    if (batch != NULL)
        rocksdb_writebatch_destroy(batch);
    
    PR_LOG("rocks_multi_insert - successfully inserted %d tuples", ntuples);
}

static TM_Result
rocks_tuple_delete(Relation relation, ItemPointer tid,
                  CommandId cid, Snapshot snapshot,
                  Snapshot crosscheck, bool wait,
                  TM_FailureData *tmfd, bool changingPart)
{
    Oid table_oid = RelationGetRelid(relation);
    uint64 rowid;
    unsigned char key[ROW_KEY_LEN];
    char *err = NULL;
    
    /* Extract row ID from ItemPointer */
    rowid = ItemPointerGetBlockNumber(tid) * BLCKSZ + ItemPointerGetOffsetNumber(tid);
    
    /* Create key for this row */
    fill_row_key(key, table_oid, rowid);
    
    if (postgresrocks_writer_available())
    {
        postgresrocks_writer_delete((const char *) key, ROW_KEY_LEN);
    }
    else
    {
        init_rocksdb(ROCKS_SESSION_WRITE);
        /* Delete from RocksDB */
        rocksdb_delete(rocks_db, rocks_write_options, (const char *) key, ROW_KEY_LEN, &err);

        if (err != NULL) {
            elog(ERROR, "RocksDB delete error: %s", err);
            free(err);
            return TM_Deleted; /* Indicate failure */
        }
    }
    
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
    Oid table_oid = RelationGetRelid(relation);
    
    PR_LOG("rocks_finish_bulk_insert - finalizing bulk insert for table_oid=%u", table_oid);
    
    /* First flush any pending write batch */
    flush_write_batch();
    
    /* Then ensure metadata is flushed */
    flush_row_counter_metadata(table_oid);
    
    PR_LOG("rocks_finish_bulk_insert - completed for table_oid=%u", table_oid);
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
    init_rocksdb(ROCKS_SESSION_WRITE);
    
    /* Create table metadata with column types */
    meta = get_table_metadata(table_oid, true);
    meta->column_count = natts;
    meta->layout = (uint32) parse_storage_layout(postgresrocks_default_layout);
    
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
    
    init_rocksdb(ROCKS_SESSION_READ);
    
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
        
        if (postgresrocks_writer_available())
        {
            postgresrocks_writer_delete(key, key_len);
        }
        else
        {
            init_rocksdb(ROCKS_SESSION_WRITE);
            rocksdb_delete(rocks_db, rocks_write_options, key, key_len, &err);
            if (err != NULL) {
                elog(WARNING, "Failed to delete key during truncate: %s", err);
                free(err);
                err = NULL;
            }
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
    PR_LOG("rocks_scan_analyze_next_block called");
    /* TODO: Implement analysis block scanning */
    return false;
}

static bool
rocks_scan_analyze_next_tuple(TableScanDesc scan,
                             TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    PR_LOG("rocks_scan_analyze_next_tuple called");
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
    PR_LOG("rocks_index_build_range_scan called");
    /* TODO: Implement index building */
    return 0.0;
}

static void
rocks_index_validate_scan(Relation tablerel, Relation indexrel,
                         IndexInfo *indexInfo, Snapshot snapshot,
                         ValidateIndexState *state)
{
    PR_LOG("rocks_index_validate_scan called");
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
    
    init_rocksdb(ROCKS_SESSION_READ);
    
    /* Create prefix for this table's data */
    prefix = make_row_prefix(table_oid, &prefix_len);
    
    /* Create iterator to scan all keys for this table */
    iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    rocksdb_iter_seek(iter, prefix, prefix_len);
    
    /* Sum up storage size of all keys and values */
    while (rocksdb_iter_valid(iter)) {
        const char *key;
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
    PR_LOG("rocks_relation_estimate_size called");
    
    /* Provide basic estimates for query planning */
    *pages = 1000;           /* Estimate 1000 pages */
    *tuples = 10000.0;       /* Estimate 10000 tuples */
    *allvisfrac = 1.0;       /* All pages are visible */
}
