#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
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
#include "storage/condition_variable.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"
#include "utils/snapmgr.h"

#include "postgresrocks_codec.h"

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
static rocksdb_flushoptions_t* rocks_flush_options = NULL;
static char *postgresrocks_default_layout = "row";
static bool postgresrocks_benchmark_write_mode = false;
static bool postgresrocks_enable_shared_writer = true;
static uint64 postgresrocks_tuple_insert_calls = 0;
static uint64 postgresrocks_multi_insert_calls = 0;
static uint64 postgresrocks_multi_inserted_tuples = 0;
uint64 postgresrocks_serialize_calls = 0;
uint64 postgresrocks_serialize_us = 0;
static uint64 postgresrocks_batch_flush_calls = 0;
static uint64 postgresrocks_batch_flush_us = 0;
static uint64 postgresrocks_batch_flush_rows = 0;
static uint64 postgresrocks_metadata_flush_calls = 0;
static uint64 postgresrocks_metadata_flush_us = 0;
static uint64 postgresrocks_rowid_calls = 0;
static uint64 postgresrocks_rowid_us = 0;
static uint64 postgresrocks_batch_put_calls = 0;
static uint64 postgresrocks_batch_put_us = 0;
static uint64 postgresrocks_tuple_insert_us = 0;
static uint64 postgresrocks_init_rocksdb_calls = 0;
static uint64 postgresrocks_init_rocksdb_us = 0;
static uint64 postgresrocks_init_lockfile_open_us = 0;
static uint64 postgresrocks_init_flock_us = 0;
static uint64 postgresrocks_init_db_open_us = 0;
static uint64 postgresrocks_init_upgrade_reopen_calls = 0;
static uint64 postgresrocks_init_upgrade_reopen_us = 0;
static uint64 postgresrocks_precommit_flush_calls = 0;
static uint64 postgresrocks_precommit_flush_us = 0;
static int rocks_lock_fd = -1;
static bool rocks_lock_held = false;
static bool rocks_exit_callback_registered = false;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static HTAB *postgresrocks_shared_table_states = NULL;
static LWLockPadded *postgresrocks_table_state_tranche = NULL;
static LWLock *postgresrocks_table_state_lock = NULL;
static LWLockPadded *postgresrocks_writer_tranche = NULL;
static LWLock *postgresrocks_writer_lock = NULL;
static pid_t postgresrocks_writer_pid = 0;

#define POSTGRESROCKS_TABLE_STATE_TRANCHE "postgresrocks_table_state"
#define POSTGRESROCKS_MAX_SHARED_TABLE_STATES 1024
#define POSTGRESROCKS_WRITER_TRANCHE "postgresrocks_writer"
#define POSTGRESROCKS_WRITER_MAX_OPS 512
#define POSTGRESROCKS_WRITER_QUEUE_SLOTS 16
#define POSTGRESROCKS_MAX_KEY_SIZE 64
#define POSTGRESROCKS_MAX_VALUE_SIZE MAX_ROW_SIZE

typedef enum RocksSessionMode
{
    ROCKS_SESSION_NONE = 0,
    ROCKS_SESSION_READ = 1,
    ROCKS_SESSION_WRITE = 2
} RocksSessionMode;
typedef struct RocksScanDesc RocksScanDesc;
static RocksSessionMode rocks_session_mode = ROCKS_SESSION_NONE;

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
    uint64 next_rowid;
    uint32 column_count;
    uint32 layout;
    uint32 storage_id;
} TableMetadata;

typedef struct RocksTupleTableSlot
{
    TupleTableSlot base;
    MemoryContext per_tuple_mcxt;
    char *row_data;
    size_t row_data_capacity;
    size_t row_data_len;
    bool row_data_external_owned;
    bool row_data_borrowed;
    bool per_tuple_context_dirty;
    RowEncodingCacheEntry *encoding_cache;
    unsigned char *null_bitmap;
    uint32 *offsets;
} RocksTupleTableSlot;

typedef struct RocksIndexFetchData
{
    IndexFetchTableData base;
    RowEncodingCacheEntry *cache;
    bool prepared;
} RocksIndexFetchData;

typedef struct RowReadCacheKey
{
    Oid storage_id;
    uint64 rowid;
} RowReadCacheKey;

typedef struct RowReadCacheEntry
{
    RowReadCacheKey key;
    size_t row_data_len;
    uint64 last_used;
    char *row_data;
} RowReadCacheEntry;

#define POSTGRESROCKS_ROW_READ_CACHE_MAX 2048
static HTAB *row_read_cache = NULL;
static uint64 row_read_cache_clock = 0;
static void init_row_read_cache(void);
static void clear_row_read_cache(void);
static void evict_one_row_read_cache_entry(void);

typedef struct SharedTableStateEntry
{
    Oid table_oid;
    uint64 row_count;
    uint64 next_rowid;
    uint32 column_count;
    uint32 layout;
    uint32 storage_id;
    bool initialized;
} SharedTableStateEntry;

typedef struct SharedWriterOp
{
    uint16 key_len;
    uint32 value_len;
    char key[POSTGRESROCKS_MAX_KEY_SIZE];
    char value[POSTGRESROCKS_MAX_VALUE_SIZE];
} SharedWriterOp;

typedef enum SharedWriterState
{
    WRITER_SLOT_IDLE = 0,
    WRITER_SLOT_FILLING = 1,
    WRITER_SLOT_READY = 2,
    WRITER_SLOT_DONE = 3,
    WRITER_SLOT_ERROR = 4
} SharedWriterState;

typedef struct SharedWriterSlot
{
    int state;
    uint64 seqno;
    int op_count;
    SharedWriterOp ops[POSTGRESROCKS_WRITER_MAX_OPS];
} SharedWriterSlot;

typedef struct SharedWriterQueue
{
    Latch worker_latch;
    ConditionVariable completion_cv;
    uint64 next_seqno;
    uint64 completed_seqno;
    uint64 failed_seqno;
    char error_message[256];
    SharedWriterSlot slots[POSTGRESROCKS_WRITER_QUEUE_SLOTS];
} SharedWriterQueue;

typedef struct PendingWriteOp
{
    uint16 key_len;
    uint32 value_len;
    char key[POSTGRESROCKS_MAX_KEY_SIZE];
    const char *value_ptr;
} PendingWriteOp;

static SharedWriterQueue *postgresrocks_writer_queue = NULL;
static PendingWriteOp *pending_write_ops = NULL;
static int pending_write_count = 0;
static int pending_write_capacity = 0;
static char *pending_write_data = NULL;
static size_t pending_write_data_used = 0;
static size_t pending_write_data_capacity = 0;

static TableMetadata *get_table_metadata_from_storage(Oid table_oid, bool create_if_missing);
static char *make_metadata_key(Oid table_oid, size_t *key_len);

#define ROW_KEY_TAG 0x01
#define ROW_KEY_LEN (1 + sizeof(uint32) + sizeof(uint32) + sizeof(uint64))
#define ROW_PREFIX_LEN (1 + sizeof(uint32) + sizeof(uint32))

static void
postgresrocks_reset_direct_stats_internal(void)
{
    postgresrocks_tuple_insert_calls = 0;
    postgresrocks_multi_insert_calls = 0;
    postgresrocks_multi_inserted_tuples = 0;
    postgresrocks_serialize_calls = 0;
    postgresrocks_serialize_us = 0;
    postgresrocks_batch_flush_calls = 0;
    postgresrocks_batch_flush_us = 0;
    postgresrocks_batch_flush_rows = 0;
    postgresrocks_metadata_flush_calls = 0;
    postgresrocks_metadata_flush_us = 0;
    postgresrocks_rowid_calls = 0;
    postgresrocks_rowid_us = 0;
    postgresrocks_batch_put_calls = 0;
    postgresrocks_batch_put_us = 0;
    postgresrocks_tuple_insert_us = 0;
    postgresrocks_init_rocksdb_calls = 0;
    postgresrocks_init_rocksdb_us = 0;
    postgresrocks_init_lockfile_open_us = 0;
    postgresrocks_init_flock_us = 0;
    postgresrocks_init_db_open_us = 0;
    postgresrocks_init_upgrade_reopen_calls = 0;
    postgresrocks_init_upgrade_reopen_us = 0;
    postgresrocks_precommit_flush_calls = 0;
    postgresrocks_precommit_flush_us = 0;
}

static inline bool
postgresrocks_shared_table_state_available(void)
{
    return postgresrocks_shared_table_states != NULL &&
           postgresrocks_table_state_lock != NULL;
}

static inline bool
postgresrocks_shared_writer_available(void)
{
    return postgresrocks_enable_shared_writer &&
           postgresrocks_writer_queue != NULL &&
           postgresrocks_writer_lock != NULL;
}

static inline bool
postgresrocks_use_shared_writer_for_relation(Relation relation)
{
    return postgresrocks_shared_writer_available() &&
           RelationGetDescr(relation)->natts <= 2;
}

static bool
postgresrocks_shared_table_state_read(Oid table_oid, uint64 *row_count, uint64 *next_rowid)
{
    SharedTableStateEntry *entry;
    bool found;

    if (!postgresrocks_shared_table_state_available())
        return false;

    LWLockAcquire(postgresrocks_table_state_lock, LW_SHARED);
    entry = (SharedTableStateEntry *) hash_search(postgresrocks_shared_table_states,
                                                  &table_oid,
                                                  HASH_FIND,
                                                  &found);
    if (!found || !entry->initialized)
    {
        LWLockRelease(postgresrocks_table_state_lock);
        return false;
    }

    if (row_count != NULL)
        *row_count = entry->row_count;
    if (next_rowid != NULL)
        *next_rowid = entry->next_rowid;
    LWLockRelease(postgresrocks_table_state_lock);
    return true;
}

static void
postgresrocks_shared_table_state_write_meta(Oid table_oid, const TableMetadata *meta)
{
    SharedTableStateEntry *entry;
    bool found;

    if (!postgresrocks_shared_table_state_available())
        return;

    LWLockAcquire(postgresrocks_table_state_lock, LW_EXCLUSIVE);
    entry = (SharedTableStateEntry *) hash_search(postgresrocks_shared_table_states,
                                                  &table_oid,
                                                  HASH_ENTER,
                                                  &found);
    entry->table_oid = table_oid;
    entry->row_count = meta->row_count;
    entry->next_rowid = meta->next_rowid;
    entry->column_count = meta->column_count;
    entry->layout = meta->layout;
    entry->storage_id = meta->storage_id;
    entry->initialized = true;
    LWLockRelease(postgresrocks_table_state_lock);
}

static void
postgresrocks_shared_table_state_write(Oid table_oid, uint64 row_count, uint64 next_rowid)
{
    TableMetadata meta;

    MemSet(&meta, 0, sizeof(meta));
    meta.row_count = row_count;
    meta.next_rowid = next_rowid;

    postgresrocks_shared_table_state_write_meta(table_oid, &meta);
}

static void
postgresrocks_shared_table_state_clear(Oid table_oid)
{
    if (!postgresrocks_shared_table_state_available())
        return;

    LWLockAcquire(postgresrocks_table_state_lock, LW_EXCLUSIVE);
    hash_search(postgresrocks_shared_table_states, &table_oid, HASH_REMOVE, NULL);
    LWLockRelease(postgresrocks_table_state_lock);
}

static bool
postgresrocks_shared_table_state_get_metadata(Oid table_oid, TableMetadata *meta)
{
    SharedTableStateEntry *entry;
    bool found;

    if (!postgresrocks_shared_table_state_available())
        return false;

    LWLockAcquire(postgresrocks_table_state_lock, LW_SHARED);
    entry = (SharedTableStateEntry *) hash_search(postgresrocks_shared_table_states,
                                                  &table_oid,
                                                  HASH_FIND,
                                                  &found);
    if (!found || !entry->initialized)
    {
        LWLockRelease(postgresrocks_table_state_lock);
        return false;
    }

    meta->row_count = entry->row_count;
    meta->next_rowid = entry->next_rowid;
    meta->column_count = entry->column_count;
    meta->layout = entry->layout;
    meta->storage_id = entry->storage_id;
    LWLockRelease(postgresrocks_table_state_lock);
    return true;
}

static uint64
postgresrocks_shared_table_state_reserve_rowids(Oid table_oid, uint64 block_size, bool create_if_missing)
{
    SharedTableStateEntry *entry;
    bool found;
    uint64 start_rowid;
    uint64 row_count;
    uint64 next_rowid;

    if (!postgresrocks_shared_table_state_available())
        return 0;

    if (!postgresrocks_shared_table_state_read(table_oid, &row_count, &next_rowid))
    {
        TableMetadata *meta = NULL;

        if (!postgresrocks_shared_writer_available())
            meta = get_table_metadata_from_storage(table_oid, create_if_missing);

        if (meta == NULL)
        {
            if (!create_if_missing)
                return 0;

            row_count = 0;
            next_rowid = 1;
        }
        else
        {
            if (meta->next_rowid == 0)
                meta->next_rowid = meta->row_count + 1;

            postgresrocks_shared_table_state_write_meta(table_oid, meta);
            row_count = meta->row_count;
            next_rowid = meta->next_rowid;
            pfree(meta);
        }
    }

    LWLockAcquire(postgresrocks_table_state_lock, LW_EXCLUSIVE);
    entry = (SharedTableStateEntry *) hash_search(postgresrocks_shared_table_states,
                                                  &table_oid,
                                                  HASH_ENTER,
                                                  &found);

    if (!entry->initialized)
    {
        entry->table_oid = table_oid;
        entry->row_count = row_count;
        entry->next_rowid = next_rowid == 0 ? row_count + 1 : next_rowid;
        entry->initialized = true;
    }

    if (entry->next_rowid == 0)
        entry->next_rowid = entry->row_count + 1;

    start_rowid = entry->next_rowid;
    entry->next_rowid += block_size;
    LWLockRelease(postgresrocks_table_state_lock);

    return start_rowid;
}

static void
postgresrocks_shmem_request(void)
{
    if (prev_shmem_request_hook != NULL)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(hash_estimate_size(POSTGRESROCKS_MAX_SHARED_TABLE_STATES,
                                              sizeof(SharedTableStateEntry)));
    RequestAddinShmemSpace(MAXALIGN(sizeof(SharedWriterQueue)));
    RequestNamedLWLockTranche(POSTGRESROCKS_TABLE_STATE_TRANCHE, 1);
    RequestNamedLWLockTranche(POSTGRESROCKS_WRITER_TRANCHE, 1);
}

static void
postgresrocks_shmem_startup(void)
{
    HASHCTL info;

    if (prev_shmem_startup_hook != NULL)
        prev_shmem_startup_hook();

    postgresrocks_table_state_tranche = GetNamedLWLockTranche(POSTGRESROCKS_TABLE_STATE_TRANCHE);
    if (postgresrocks_table_state_tranche == NULL)
        elog(ERROR, "Failed to initialize postgresrocks LWLock tranche");

    postgresrocks_table_state_lock = &(postgresrocks_table_state_tranche[0].lock);
    postgresrocks_writer_tranche = GetNamedLWLockTranche(POSTGRESROCKS_WRITER_TRANCHE);
    if (postgresrocks_writer_tranche == NULL)
        elog(ERROR, "Failed to initialize postgresrocks writer LWLock tranche");
    postgresrocks_writer_lock = &(postgresrocks_writer_tranche[0].lock);

    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(SharedTableStateEntry);

    postgresrocks_shared_table_states =
        ShmemInitHash("PostgresRocks shared table states",
                      POSTGRESROCKS_MAX_SHARED_TABLE_STATES,
                      POSTGRESROCKS_MAX_SHARED_TABLE_STATES,
                      &info,
                      HASH_ELEM | HASH_BLOBS);

    {
        bool found = false;

        int i;

        postgresrocks_writer_queue = (SharedWriterQueue *)
            ShmemInitStruct("PostgresRocks shared writer queue",
                            sizeof(SharedWriterQueue),
                            &found);
        if (!found)
        {
            MemSet(postgresrocks_writer_queue, 0, sizeof(SharedWriterQueue));
            InitSharedLatch(&postgresrocks_writer_queue->worker_latch);
            ConditionVariableInit(&postgresrocks_writer_queue->completion_cv);
            for (i = 0; i < POSTGRESROCKS_WRITER_QUEUE_SLOTS; i++)
            {
                postgresrocks_writer_queue->slots[i].state = WRITER_SLOT_IDLE;
                postgresrocks_writer_queue->slots[i].seqno = 0;
            }
        }
    }
}

static TableMetadata *get_table_metadata(Oid table_oid, bool create_if_missing);
static void update_table_metadata(Oid table_oid, TableMetadata *meta);
static void persist_table_metadata_sync(Oid table_oid, TableMetadata *meta);
static void postgresrocks_xact_callback(XactEvent event, void *arg);
static void close_rocksdb_session(void);
static void postgresrocks_backend_exit(int code, Datum arg);
static void postgresrocks_shmem_request(void);
static void postgresrocks_shmem_startup(void);
static void init_rocksdb(RocksSessionMode requested_mode);
static void clear_row_counter_cache(void);
static void clear_row_counter_entry(Oid table_oid);
static uint64 delete_table_storage_internal(Oid table_oid, bool delete_metadata);
static uint64 delete_table_storage_for_identity(Oid table_oid, Oid storage_id, bool delete_metadata);
static void postgresrocks_reset_direct_stats_internal(void);
static TableMetadata *get_table_metadata_from_storage(Oid table_oid, bool create_if_missing);
extern PGDLLEXPORT void postgresrocks_writer_main(Datum main_arg);
static void ensure_pending_write_capacity(int needed);
static void ensure_pending_write_data_capacity(size_t needed);
static void queue_pending_write(const char *key, uint16 key_len, const char *value, uint32 value_len);
static void reset_pending_writes(void);
static void flush_pending_writes_to_shared_writer(void);
static void submit_metadata_to_shared_writer(Oid table_oid, const TableMetadata *meta);
static uint64 enqueue_ops_to_shared_writer(PendingWriteOp *ops, int op_count);
static void wait_for_shared_writer_target(uint64 target_seqno);
static void ensure_writes_flushed(void);
static StrategyNumber rocks_fast_filter_strategy_from_scankey(ScanKey key);
static bool rocks_decode_integral_attr_from_row(const char *row_data,
                                                RowEncodingCacheEntry *cache,
                                                AttrNumber attno,
                                                AttrCodecKind *kind,
                                                int64 *value,
                                                bool *isnull);
static bool rocks_row_matches_fast_filters(const char *row_data,
                                           RocksScanDesc *scan);
static void rocks_init_fast_filters(RocksScanDesc *scan,
                                    Relation relation,
                                    int nkeys,
                                    ScanKey key);

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

static void
ensure_pending_write_capacity(int needed)
{
    int old_capacity = pending_write_capacity;

    if (pending_write_capacity >= needed)
        return;

    if (pending_write_ops == NULL)
        pending_write_ops = MemoryContextAllocZero(TopMemoryContext,
                                                   sizeof(PendingWriteOp) * needed);
    else
    {
        pending_write_ops = repalloc(pending_write_ops,
                                     sizeof(PendingWriteOp) * needed);
        MemSet(pending_write_ops + old_capacity,
               0,
               sizeof(PendingWriteOp) * (needed - old_capacity));
    }

    pending_write_capacity = needed;
}

static void
ensure_pending_write_data_capacity(size_t needed)
{
    size_t new_capacity;

    if (pending_write_data_capacity >= needed)
        return;

    new_capacity = pending_write_data_capacity == 0 ? (size_t) (POSTGRESROCKS_WRITER_MAX_OPS * 256) :
                                                      pending_write_data_capacity * 2;
    while (new_capacity < needed)
        new_capacity *= 2;

    if (pending_write_data == NULL)
        pending_write_data = MemoryContextAlloc(TopMemoryContext, new_capacity);
    else
        pending_write_data = repalloc(pending_write_data, new_capacity);

    pending_write_data_capacity = new_capacity;
}

static void
queue_pending_write(const char *key, uint16 key_len, const char *value, uint32 value_len)
{
    PendingWriteOp *op;

    if (key_len > POSTGRESROCKS_MAX_KEY_SIZE)
        elog(ERROR, "postgresrocks shared writer key too large: %u", key_len);
    if (value_len > POSTGRESROCKS_MAX_VALUE_SIZE)
        elog(ERROR, "postgresrocks shared writer value too large: %u", value_len);

    ensure_pending_write_capacity(pending_write_count + 256);
    ensure_pending_write_data_capacity(pending_write_data_used + value_len);
    op = &pending_write_ops[pending_write_count++];
    op->key_len = key_len;
    op->value_len = value_len;
    memcpy(op->key, key, key_len);
    op->value_ptr = pending_write_data + pending_write_data_used;
    memcpy(pending_write_data + pending_write_data_used, value, value_len);
    pending_write_data_used += value_len;
}

static void
reset_pending_writes(void)
{
    int i;

    for (i = 0; i < pending_write_count; i++)
    {
        pending_write_ops[i].key_len = 0;
        pending_write_ops[i].value_len = 0;
        pending_write_ops[i].value_ptr = NULL;
    }

    pending_write_count = 0;
    pending_write_data_used = 0;
}

static void
postgresrocks_register_writer(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        return;

    MemSet(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "postgresrocks shared writer");
    snprintf(worker.bgw_type, BGW_MAXLEN, "postgresrocks_shared_writer");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    snprintf(worker.bgw_library_name, MAXPGPATH, "postgresrocks");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "postgresrocks_writer_main");
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}

static uint64
enqueue_ops_to_shared_writer(PendingWriteOp *ops, int op_count)
{
    int slot_idx = -1;
    int i;
    uint64 seqno = 0;

    if (!postgresrocks_shared_writer_available())
        elog(ERROR, "postgresrocks shared writer is not available");

    LWLockAcquire(postgresrocks_writer_lock, LW_EXCLUSIVE);
    for (i = 0; i < POSTGRESROCKS_WRITER_QUEUE_SLOTS; i++)
    {
        if (postgresrocks_writer_queue->slots[i].state == WRITER_SLOT_IDLE)
        {
            SharedWriterSlot *slot = &postgresrocks_writer_queue->slots[i];
            slot_idx = i;
            slot->op_count = 0;
            slot->seqno = ++postgresrocks_writer_queue->next_seqno;
            seqno = slot->seqno;
            slot->state = WRITER_SLOT_FILLING;
            break;
        }
    }
    LWLockRelease(postgresrocks_writer_lock);

    if (slot_idx >= 0)
    {
        SharedWriterSlot *slot = &postgresrocks_writer_queue->slots[slot_idx];

        for (i = 0; i < op_count; i++)
        {
            slot->ops[i].key_len = ops[i].key_len;
            slot->ops[i].value_len = ops[i].value_len;
            memcpy(slot->ops[i].key, ops[i].key, ops[i].key_len);
            memcpy(slot->ops[i].value, ops[i].value_ptr, ops[i].value_len);
        }

        LWLockAcquire(postgresrocks_writer_lock, LW_EXCLUSIVE);
        slot->op_count = op_count;
        slot->state = WRITER_SLOT_READY;
        LWLockRelease(postgresrocks_writer_lock);
        SetLatch(&postgresrocks_writer_queue->worker_latch);
    }

    return seqno;
}

static void
wait_for_shared_writer_target(uint64 target_seqno)
{
    char error_message[256];

    error_message[0] = '\0';
    ConditionVariablePrepareToSleep(&postgresrocks_writer_queue->completion_cv);

    for (;;)
    {
        CHECK_FOR_INTERRUPTS();
        LWLockAcquire(postgresrocks_writer_lock, LW_SHARED);
        if (postgresrocks_writer_queue->failed_seqno != 0 &&
            postgresrocks_writer_queue->failed_seqno <= target_seqno)
        {
            strlcpy(error_message, postgresrocks_writer_queue->error_message, sizeof(error_message));
            LWLockRelease(postgresrocks_writer_lock);
            break;
        }
        if (postgresrocks_writer_queue->completed_seqno >= target_seqno)
        {
            LWLockRelease(postgresrocks_writer_lock);
            ConditionVariableCancelSleep();
            return;
        }
        LWLockRelease(postgresrocks_writer_lock);
        ConditionVariableSleep(&postgresrocks_writer_queue->completion_cv, 0);
    }
    ConditionVariableCancelSleep();
    elog(ERROR, "postgresrocks shared writer error: %s",
         error_message[0] != '\0' ? error_message : "unknown error");
}

static void
flush_pending_writes_to_shared_writer(void)
{
    int offset = 0;
    uint64 target_seqno = 0;

    if (pending_write_count == 0)
        return;

    while (offset < pending_write_count)
    {
        int chunk = Min(POSTGRESROCKS_WRITER_MAX_OPS, pending_write_count - offset);
        uint64 seqno = enqueue_ops_to_shared_writer(&pending_write_ops[offset], chunk);

        if (seqno == 0)
        {
            if (target_seqno != 0)
                wait_for_shared_writer_target(target_seqno);
            else
                pg_usleep(1000L);
            continue;
        }

        target_seqno = seqno;
        offset += chunk;
    }

    if (target_seqno != 0)
        wait_for_shared_writer_target(target_seqno);

    reset_pending_writes();
}

static void
submit_metadata_to_shared_writer(Oid table_oid, const TableMetadata *meta)
{
    PendingWriteOp op;
    char *key;
    size_t key_len;

    key = make_metadata_key(table_oid, &key_len);
    MemSet(&op, 0, sizeof(op));
    op.key_len = (uint16) key_len;
    op.value_len = sizeof(TableMetadata);
    memcpy(op.key, key, key_len);
    op.value_ptr = (const char *) meta;
    while (true)
    {
        uint64 seqno = enqueue_ops_to_shared_writer(&op, 1);

        if (seqno != 0)
        {
            wait_for_shared_writer_target(seqno);
            break;
        }

        pg_usleep(1000L);
    }
    pfree(key);
}

void
_PG_init(void)
{
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

    DefineCustomBoolVariable("postgresrocks.enable_shared_writer",
                             "Route insert batches through the shared postgresrocks writer.",
                             "Requires shared_preload_libraries='postgresrocks'. This experimental path avoids reopening RocksDB in write mode per transaction.",
                             &postgresrocks_enable_shared_writer,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    if (shmem_request_hook != postgresrocks_shmem_request)
    {
        prev_shmem_request_hook = shmem_request_hook;
        shmem_request_hook = postgresrocks_shmem_request;
    }
    if (shmem_startup_hook != postgresrocks_shmem_startup)
    {
        prev_shmem_startup_hook = shmem_startup_hook;
        shmem_startup_hook = postgresrocks_shmem_startup;
    }

    postgresrocks_register_writer();
    RegisterXactCallback(postgresrocks_xact_callback, NULL);
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

static inline uint64
decode_u64_be(const unsigned char *src)
{
    return ((uint64) src[0] << 56) |
           ((uint64) src[1] << 48) |
           ((uint64) src[2] << 40) |
           ((uint64) src[3] << 32) |
           ((uint64) src[4] << 24) |
           ((uint64) src[5] << 16) |
           ((uint64) src[6] << 8) |
           (uint64) src[7];
}

static inline uint64
tid_to_rowid(ItemPointer tid)
{
    return (uint64) itemptr_encode(tid);
}

static inline void
rowid_to_tid(uint64 rowid, ItemPointer tid)
{
    itemptr_decode(tid, (int64) rowid);
}

static inline Oid
rocks_relation_storage_id(Relation rel)
{
    return rel->rd_locator.relNumber;
}

static Oid
rocks_storage_id_by_table_oid(Oid table_oid)
{
    Relation rel;
    Oid storage_id;

    rel = table_open(table_oid, AccessShareLock);
    storage_id = rocks_relation_storage_id(rel);
    table_close(rel, AccessShareLock);

    return storage_id;
}

static void
postgresrocks_fill_metadata_storage_id(Oid table_oid, TableMetadata *meta)
{
    if (meta == NULL || meta->storage_id != 0)
        return;

    meta->storage_id = (uint32) rocks_storage_id_by_table_oid(table_oid);
}

static Oid
postgresrocks_lookup_storage_id_for_cleanup(Oid table_oid)
{
    TableMetadata *meta;
    Oid storage_id = InvalidOid;

    meta = get_table_metadata_from_storage(table_oid, false);
    if (meta != NULL)
    {
        storage_id = (Oid) meta->storage_id;
        pfree(meta);
    }

    return storage_id;
}

static char *
make_row_key(Oid table_oid, Oid storage_id, uint64 rowid, size_t *key_len)
{
    unsigned char *key = (unsigned char *) palloc(ROW_KEY_LEN);

    key[0] = ROW_KEY_TAG;
    encode_u32_be(key + 1, (uint32) table_oid);
    encode_u32_be(key + 1 + sizeof(uint32), (uint32) storage_id);
    encode_u64_be(key + 1 + sizeof(uint32) + sizeof(uint32), rowid);
    *key_len = ROW_KEY_LEN;

    return (char *) key;
}

static inline void
fill_row_key(unsigned char *key, Oid table_oid, Oid storage_id, uint64 rowid)
{
    key[0] = ROW_KEY_TAG;
    encode_u32_be(key + 1, (uint32) table_oid);
    encode_u32_be(key + 1 + sizeof(uint32), (uint32) storage_id);
    encode_u64_be(key + 1 + sizeof(uint32) + sizeof(uint32), rowid);
}

/* Helper function to create key prefix for table scanning */
static char *
make_row_prefix(Oid table_oid, Oid storage_id, size_t *prefix_len)
{
    unsigned char *prefix = (unsigned char *) palloc(ROW_PREFIX_LEN);

    prefix[0] = ROW_KEY_TAG;
    encode_u32_be(prefix + 1, (uint32) table_oid);
    encode_u32_be(prefix + 1 + sizeof(uint32), (uint32) storage_id);
    *prefix_len = ROW_PREFIX_LEN;

    return (char *) prefix;
}

static void
init_row_read_cache(void)
{
    HASHCTL info;

    if (row_read_cache != NULL)
        return;

    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(RowReadCacheKey);
    info.entrysize = sizeof(RowReadCacheEntry);
    info.hcxt = TopMemoryContext;

    row_read_cache = hash_create("postgresrocks row read cache",
                                 POSTGRESROCKS_ROW_READ_CACHE_MAX,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
clear_row_read_cache(void)
{
    HASH_SEQ_STATUS status;
    RowReadCacheEntry *entry;

    if (row_read_cache == NULL)
        return;

    hash_seq_init(&status, row_read_cache);
    while ((entry = (RowReadCacheEntry *) hash_seq_search(&status)) != NULL)
    {
        if (entry->row_data != NULL)
            pfree(entry->row_data);
    }

    hash_destroy(row_read_cache);
    row_read_cache = NULL;
    row_read_cache_clock = 0;
}

static void
evict_one_row_read_cache_entry(void)
{
    HASH_SEQ_STATUS status;
    RowReadCacheEntry *entry;
    RowReadCacheEntry *oldest = NULL;
    bool found;

    if (row_read_cache == NULL || hash_get_num_entries(row_read_cache) < POSTGRESROCKS_ROW_READ_CACHE_MAX)
        return;

    hash_seq_init(&status, row_read_cache);
    while ((entry = (RowReadCacheEntry *) hash_seq_search(&status)) != NULL)
    {
        if (oldest == NULL || entry->last_used < oldest->last_used)
            oldest = entry;
    }

    if (oldest != NULL)
    {
        RowReadCacheKey key = oldest->key;

        if (oldest->row_data != NULL)
            pfree(oldest->row_data);
        hash_search(row_read_cache, &key, HASH_REMOVE, &found);
    }
}

static Oid
resolve_table_oid_by_name(const char *table_name)
{
    Oid table_oid;
    Oid namespaceId;
    RangeVar *relvar;

    namespaceId = get_namespace_oid("public", true);
    if (namespaceId != InvalidOid)
        table_oid = get_relname_relid(table_name, namespaceId);
    else
        table_oid = InvalidOid;

    if (table_oid != InvalidOid)
        return table_oid;

    relvar = palloc0(sizeof(RangeVar));
    relvar->schemaname = NULL;
    relvar->relname = pstrdup(table_name);
    relvar->inh = true;
    relvar->relpersistence = RELPERSISTENCE_PERMANENT;
    relvar->location = -1;

    table_oid = RangeVarGetRelid(relvar, NoLock, true);

    pfree(relvar->relname);
    pfree(relvar);

    return table_oid;
}

static char *
fetch_row_data_by_rowid(Oid table_oid, uint64 rowid, size_t *row_data_len)
{
    Oid storage_id = rocks_storage_id_by_table_oid(table_oid);
    unsigned char key_buf[ROW_KEY_LEN];
    char *err = NULL;
    char *row_data;

    fill_row_key(key_buf, table_oid, storage_id, rowid);
    row_data = rocksdb_get(rocks_db,
                           rocks_read_options,
                           (const char *) key_buf,
                           ROW_KEY_LEN,
                           row_data_len,
                           &err);

    if (err != NULL)
    {
        char *err_copy = pstrdup(err);

        free(err);
        ereport(ERROR,
                (errmsg("Failed to read row %llu for table %u: %s",
                        (unsigned long long) rowid,
                        table_oid,
                        err_copy)));
    }

    return row_data;
}

static const char *
fetch_cached_row_data_by_rowid(Oid table_oid, uint64 rowid, size_t *row_data_len)
{
    RowReadCacheKey key;
    RowReadCacheEntry *entry;
    bool found;
    char *row_data;
    size_t len;
    Oid storage_id = rocks_storage_id_by_table_oid(table_oid);

    init_row_read_cache();

    key.storage_id = storage_id;
    key.rowid = rowid;
    entry = (RowReadCacheEntry *) hash_search(row_read_cache, &key, HASH_FIND, &found);
    if (found)
    {
        entry->last_used = ++row_read_cache_clock;
        *row_data_len = entry->row_data_len;
        return entry->row_data;
    }

    row_data = fetch_row_data_by_rowid(table_oid, rowid, &len);
    if (row_data == NULL)
    {
        *row_data_len = 0;
        return NULL;
    }

    evict_one_row_read_cache_entry();
    entry = (RowReadCacheEntry *) hash_search(row_read_cache, &key, HASH_ENTER, &found);
    entry->key = key;
    entry->row_data_len = len;
    entry->last_used = ++row_read_cache_clock;
    entry->row_data = MemoryContextAlloc(TopMemoryContext, len);
    memcpy(entry->row_data, row_data, len);
    free(row_data);

    *row_data_len = entry->row_data_len;
    return entry->row_data;
}

static inline void
rocks_reset_row_storage(RocksTupleTableSlot *rslot)
{
    if (rslot->row_data_external_owned && rslot->row_data != NULL)
        free(rslot->row_data);
    else if (!rslot->row_data_borrowed && rslot->row_data != NULL)
        pfree(rslot->row_data);

    rslot->row_data = NULL;
    rslot->row_data_capacity = 0;
    rslot->row_data_external_owned = false;
    rslot->row_data_borrowed = false;
    rslot->row_data_len = 0;
    rslot->per_tuple_context_dirty = false;
    rslot->encoding_cache = NULL;
    rslot->null_bitmap = NULL;
    rslot->offsets = NULL;
}

static inline void
rocks_store_lazy_row(TupleTableSlot *slot, const char *row_data, size_t row_data_len,
                     RowEncodingCacheEntry *cache, uint64 rowid)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;
    RowHeader *header;

    ExecClearTuple(slot);
    rocks_reset_row_storage(rslot);

    if (rslot->row_data_capacity < row_data_len)
    {
        if (rslot->row_data == NULL)
            rslot->row_data = MemoryContextAlloc(slot->tts_mcxt, row_data_len);
        else
            rslot->row_data = repalloc(rslot->row_data, row_data_len);
        rslot->row_data_capacity = row_data_len;
    }

    memcpy(rslot->row_data, row_data, row_data_len);
    rslot->row_data_len = row_data_len;
    rslot->encoding_cache = cache;

    header = (RowHeader *) rslot->row_data;
    if (header->natts != cache->natts)
    {
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u",
             cache->natts, header->natts);
    }

    rslot->null_bitmap = (unsigned char *) (rslot->row_data + sizeof(RowHeader));
    rslot->offsets = (uint32 *) ((char *) rslot->null_bitmap + cache->nullmap_size);

    rowid_to_tid(rowid, &slot->tts_tid);
    slot->tts_nvalid = 0;
    slot->tts_flags &= ~TTS_FLAG_EMPTY;
}

static inline void
rocks_store_owned_row(TupleTableSlot *slot, char *row_data, size_t row_data_len,
                      RowEncodingCacheEntry *cache, uint64 rowid)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;
    RowHeader *header;

    ExecClearTuple(slot);
    rocks_reset_row_storage(rslot);

    rslot->row_data = row_data;
    rslot->row_data_len = row_data_len;
    rslot->row_data_capacity = row_data_len;
    rslot->row_data_external_owned = true;
    rslot->encoding_cache = cache;

    header = (RowHeader *) rslot->row_data;
    if (header->natts != cache->natts)
    {
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u",
             cache->natts, header->natts);
    }

    rslot->null_bitmap = (unsigned char *) (rslot->row_data + sizeof(RowHeader));
    rslot->offsets = (uint32 *) ((char *) rslot->null_bitmap + cache->nullmap_size);

    rowid_to_tid(rowid, &slot->tts_tid);
    slot->tts_nvalid = 0;
    slot->tts_flags &= ~TTS_FLAG_EMPTY;
}

static inline void
rocks_store_borrowed_row(TupleTableSlot *slot, const char *row_data, size_t row_data_len,
                         RowEncodingCacheEntry *cache, uint64 rowid)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;
    RowHeader *header;

    ExecClearTuple(slot);
    rocks_reset_row_storage(rslot);

    rslot->row_data = (char *) row_data;
    rslot->row_data_len = row_data_len;
    rslot->row_data_borrowed = true;
    rslot->encoding_cache = cache;

    header = (RowHeader *) rslot->row_data;
    if (header->natts != cache->natts)
    {
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u",
             cache->natts, header->natts);
    }

    rslot->null_bitmap = (unsigned char *) (rslot->row_data + sizeof(RowHeader));
    rslot->offsets = (uint32 *) ((char *) rslot->null_bitmap + cache->nullmap_size);

    rowid_to_tid(rowid, &slot->tts_tid);
    slot->tts_nvalid = 0;
    slot->tts_flags &= ~TTS_FLAG_EMPTY;
}

/* Helper function to create metadata key */
static char *
make_metadata_key(Oid table_oid, size_t *key_len)
{
    char *key = palloc(64);
    *key_len = snprintf(key, 64, "meta_%u_info", table_oid);
    return key;
}

/* Custom scan descriptor for row storage */
typedef struct RocksFastFilter
{
    AttrNumber attno;
    AttrCodecKind kind;
    StrategyNumber strategy;
    int64 compare_value;
} RocksFastFilter;

typedef struct RocksScanDesc
{
    TableScanDescData rs_base;
    char *key_prefix;
    size_t key_prefix_len;
    rocksdb_iterator_t *iter;
    RowEncodingCacheEntry *encoding_cache;
    uint64 current_rowid;
    uint64 total_rows;
    bool started;
    bool analyze_block_reported;
    int fast_filter_count;
    RocksFastFilter *fast_filters;
} RocksScanDesc;

static StrategyNumber
rocks_fast_filter_strategy_from_scankey(ScanKey key)
{
    if (key->sk_strategy >= BTLessStrategyNumber &&
        key->sk_strategy <= BTGreaterStrategyNumber)
        return key->sk_strategy;

    switch (key->sk_func.fn_oid)
    {
        case F_INT4LT:
        case F_INT8LT:
            return BTLessStrategyNumber;
        case F_INT4LE:
        case F_INT8LE:
            return BTLessEqualStrategyNumber;
        case F_INT4EQ:
        case F_INT8EQ:
            return BTEqualStrategyNumber;
        case F_INT4GE:
        case F_INT8GE:
            return BTGreaterEqualStrategyNumber;
        case F_INT4GT:
        case F_INT8GT:
            return BTGreaterStrategyNumber;
        default:
            return InvalidStrategy;
    }
}

static bool
rocks_decode_integral_attr_from_row(const char *row_data,
                                    RowEncodingCacheEntry *cache,
                                    AttrNumber attno,
                                    AttrCodecKind *kind,
                                    int64 *value,
                                    bool *isnull)
{
    const RowHeader *header = (const RowHeader *) row_data;
    const unsigned char *null_bitmap;
    const uint32 *offsets;
    int target_index = attno - 1;
    const char *attr_ptr;

    if (attno <= 0 || attno > cache->natts)
        return false;

    if (header->natts != cache->natts)
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u",
             cache->natts, header->natts);

    null_bitmap = (const unsigned char *) (row_data + sizeof(RowHeader));
    if (row_attr_is_null(null_bitmap, target_index))
    {
        *isnull = true;
        return true;
    }

    offsets = (const uint32 *) ((const char *) null_bitmap + cache->nullmap_size);
    if (offsets[target_index] == ROW_ATTR_OFFSET_NULL)
    {
        *isnull = true;
        return true;
    }

    *kind = cache->codecs[target_index].kind;
    *isnull = false;
    attr_ptr = row_data + offsets[target_index];

    switch (cache->codecs[target_index].kind)
    {
        case ATTR_CODEC_INT4:
        {
            int32 v;

            memcpy(&v, attr_ptr, sizeof(int32));
            *value = v;
            return true;
        }
        case ATTR_CODEC_INT8:
        {
            int64 v;

            memcpy(&v, attr_ptr, sizeof(int64));
            *value = v;
            return true;
        }
        default:
            return false;
    }
}

static bool
rocks_row_matches_fast_filters(const char *row_data, RocksScanDesc *scan)
{
    int i;

    for (i = 0; i < scan->fast_filter_count; i++)
    {
        RocksFastFilter *filter = &scan->fast_filters[i];
        AttrCodecKind kind = 0;
        int64 value = 0;
        bool isnull = false;
        bool decoded;

        decoded = rocks_decode_integral_attr_from_row(row_data,
                                                      scan->encoding_cache,
                                                      filter->attno,
                                                      &kind,
                                                      &value,
                                                      &isnull);
        if (!decoded)
            continue;

        if (isnull || kind != filter->kind)
            return false;

        switch (filter->strategy)
        {
            case BTLessStrategyNumber:
                if (!(value < filter->compare_value))
                    return false;
                break;
            case BTLessEqualStrategyNumber:
                if (!(value <= filter->compare_value))
                    return false;
                break;
            case BTEqualStrategyNumber:
                if (!(value == filter->compare_value))
                    return false;
                break;
            case BTGreaterEqualStrategyNumber:
                if (!(value >= filter->compare_value))
                    return false;
                break;
            case BTGreaterStrategyNumber:
                if (!(value > filter->compare_value))
                    return false;
                break;
            default:
                break;
        }
    }

    return true;
}

static void
rocks_init_fast_filters(RocksScanDesc *scan,
                        Relation relation,
                        int nkeys,
                        ScanKey key)
{
    TupleDesc tupdesc = RelationGetDescr(relation);
    int i;
    int supported = 0;

    scan->fast_filter_count = 0;
    scan->fast_filters = NULL;

    if (nkeys <= 0 || key == NULL)
        return;

    scan->fast_filters = palloc0(sizeof(RocksFastFilter) * nkeys);

    for (i = 0; i < nkeys; i++)
    {
        ScanKey skey = &key[i];
        AttrNumber attno = skey->sk_attno;
        StrategyNumber strategy;
        Form_pg_attribute attr;
        RocksFastFilter *filter;

        if (attno <= 0 || attno > tupdesc->natts)
            continue;
        if (skey->sk_flags & (SK_ISNULL | SK_SEARCHNULL | SK_SEARCHNOTNULL |
                              SK_SEARCHARRAY | SK_ROW_HEADER | SK_ROW_MEMBER |
                              SK_ORDER_BY))
            continue;

        strategy = rocks_fast_filter_strategy_from_scankey(skey);
        if (strategy == InvalidStrategy)
            continue;

        attr = TupleDescAttr(tupdesc, attno - 1);
        filter = &scan->fast_filters[supported];
        filter->attno = attno;
        filter->strategy = strategy;

        switch (attr->atttypid)
        {
            case INT4OID:
                filter->kind = ATTR_CODEC_INT4;
                filter->compare_value = (int64) DatumGetInt32(skey->sk_argument);
                supported++;
                break;
            case INT8OID:
                filter->kind = ATTR_CODEC_INT8;
                filter->compare_value = DatumGetInt64(skey->sk_argument);
                supported++;
                break;
            default:
                break;
        }
    }

    if (supported == 0)
    {
        pfree(scan->fast_filters);
        scan->fast_filters = NULL;
        return;
    }

    scan->fast_filter_count = supported;
}

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

static void
rocks_lazy_slot_init(TupleTableSlot *slot)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;

    rslot->per_tuple_mcxt = AllocSetContextCreate(slot->tts_mcxt,
                                                  "postgresrocks slot tuple ctx",
                                                  ALLOCSET_SMALL_SIZES);
    rslot->row_data = NULL;
    rslot->row_data_capacity = 0;
    rslot->row_data_len = 0;
    rslot->row_data_external_owned = false;
    rslot->row_data_borrowed = false;
    rslot->per_tuple_context_dirty = false;
    rslot->encoding_cache = NULL;
    rslot->null_bitmap = NULL;
    rslot->offsets = NULL;
}

static void
rocks_lazy_slot_release(TupleTableSlot *slot)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;

    if (rslot->row_data_external_owned && rslot->row_data != NULL)
        free(rslot->row_data);
    if (rslot->per_tuple_mcxt != NULL)
        MemoryContextDelete(rslot->per_tuple_mcxt);
}

static void
rocks_lazy_slot_clear(TupleTableSlot *slot)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;

    if (rslot->per_tuple_context_dirty)
        MemoryContextReset(rslot->per_tuple_mcxt);
    rocks_reset_row_storage(rslot);
    slot->tts_nvalid = 0;
    slot->tts_flags |= TTS_FLAG_EMPTY;
}

static void
rocks_lazy_slot_getsomeattrs(TupleTableSlot *slot, int natts)
{
    RocksTupleTableSlot *rslot = (RocksTupleTableSlot *) slot;
    RowEncodingCacheEntry *cache = rslot->encoding_cache;
    int i;

    if (cache == NULL)
        elog(ERROR, "postgresrocks lazy slot has no row loaded");

    if (natts > cache->natts)
        natts = cache->natts;

    for (i = slot->tts_nvalid; i < natts; i++)
    {
        slot->tts_isnull[i] = row_attr_is_null(rslot->null_bitmap, i);

        if (slot->tts_isnull[i])
        {
            slot->tts_values[i] = (Datum) 0;
            continue;
        }

        if (rslot->offsets[i] == ROW_ATTR_OFFSET_NULL)
        {
            slot->tts_values[i] = (Datum) 0;
            slot->tts_isnull[i] = true;
            continue;
        }

        {
            const char *attr_ptr = rslot->row_data + rslot->offsets[i];

            if (cache->codecs[i].kind == ATTR_CODEC_TEXT)
                rslot->per_tuple_context_dirty = true;
            slot->tts_values[i] = cache->codecs[i].decode(&cache->codecs[i],
                                                          &attr_ptr,
                                                          &slot->tts_isnull[i],
                                                          rslot->per_tuple_mcxt);
        }
    }

    slot->tts_nvalid = natts;
}

static Datum
rocks_lazy_slot_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    elog(ERROR, "postgresrocks slots do not support system attribute %d", attnum);
    *isnull = true;
    return (Datum) 0;
}

static bool
rocks_lazy_slot_is_current_xact_tuple(TupleTableSlot *slot)
{
    elog(ERROR, "postgresrocks lazy slots do not support is_current_xact_tuple");
    return false;
}

static void
rocks_lazy_slot_materialize(TupleTableSlot *slot)
{
    slot_getallattrs(slot);
}

static void
rocks_lazy_slot_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
    HeapTuple tuple;

    slot_getallattrs(srcslot);
    tuple = heap_form_tuple(srcslot->tts_tupleDescriptor,
                            srcslot->tts_values,
                            srcslot->tts_isnull);
    ExecClearTuple(dstslot);
    ExecForceStoreHeapTuple(tuple, dstslot, true);
}

static HeapTuple
rocks_lazy_slot_copy_heap_tuple(TupleTableSlot *slot)
{
    slot_getallattrs(slot);
    return heap_form_tuple(slot->tts_tupleDescriptor,
                           slot->tts_values,
                           slot->tts_isnull);
}

static MinimalTuple
rocks_lazy_slot_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
{
    HeapTuple tuple;
    MinimalTuple mtup;
    Size len;

    tuple = rocks_lazy_slot_copy_heap_tuple(slot);
    len = tuple->t_len - MINIMAL_TUPLE_OFFSET;
    mtup = (MinimalTuple) palloc0(len + extra);
    memcpy(mtup, (char *) tuple->t_data + MINIMAL_TUPLE_OFFSET, len);
    pfree(tuple);
    return mtup;
}

static const TupleTableSlotOps TTSOpsPostgresRocksLazy = {
    .base_slot_size = sizeof(RocksTupleTableSlot),
    .init = rocks_lazy_slot_init,
    .release = rocks_lazy_slot_release,
    .clear = rocks_lazy_slot_clear,
    .getsomeattrs = rocks_lazy_slot_getsomeattrs,
    .getsysattr = rocks_lazy_slot_getsysattr,
    .is_current_xact_tuple = rocks_lazy_slot_is_current_xact_tuple,
    .materialize = rocks_lazy_slot_materialize,
    .copyslot = rocks_lazy_slot_copyslot,
    .get_heap_tuple = NULL,
    .get_minimal_tuple = NULL,
    .copy_heap_tuple = rocks_lazy_slot_copy_heap_tuple,
    .copy_minimal_tuple = rocks_lazy_slot_copy_minimal_tuple
};

    
/* Function to initialize RocksDB */
static void
init_rocksdb(RocksSessionMode requested_mode)
{
    char *err = NULL;
    int lock_mode;
    TimestampTz start_ts = GetCurrentTimestamp();
    TimestampTz sub_start;

    if (requested_mode == ROCKS_SESSION_NONE)
        requested_mode = ROCKS_SESSION_READ;

    if (rocks_db != NULL)
    {
        if (rocks_session_mode >= requested_mode)
            return;

        /* Upgrade read session to write session by reopening under exclusive lock. */
        sub_start = GetCurrentTimestamp();
        close_rocksdb_session();
        postgresrocks_init_upgrade_reopen_calls++;
        postgresrocks_init_upgrade_reopen_us += TimestampDifferenceMicroseconds(sub_start, GetCurrentTimestamp());
    }

    if (rocks_lock_fd == -1)
    {
        sub_start = GetCurrentTimestamp();
        rocks_lock_fd = open("postgresrocks_data.pglock", O_RDWR | O_CREAT, 0600);
        if (rocks_lock_fd < 0)
            elog(ERROR, "Failed to open postgresrocks coordination lock file: %m");
        postgresrocks_init_lockfile_open_us += TimestampDifferenceMicroseconds(sub_start, GetCurrentTimestamp());
    }

    if (!rocks_lock_held)
    {
        lock_mode = (requested_mode == ROCKS_SESSION_WRITE) ? LOCK_EX : LOCK_SH;
        sub_start = GetCurrentTimestamp();
        if (flock(rocks_lock_fd, lock_mode) != 0)
            elog(ERROR, "Failed to acquire postgresrocks coordination lock: %m");
        rocks_lock_held = true;
        postgresrocks_init_flock_us += TimestampDifferenceMicroseconds(sub_start, GetCurrentTimestamp());
    }

    if (rocks_options == NULL)
    {
        rocks_options = rocksdb_options_create();
        rocksdb_options_set_create_if_missing(rocks_options, 1);
        rocksdb_options_set_compression(rocks_options, rocksdb_no_compression);
        rocksdb_options_increase_parallelism(rocks_options, 4);
        rocksdb_options_optimize_level_style_compaction(rocks_options, 128ULL * 1024ULL * 1024ULL);
        rocksdb_options_set_write_buffer_size(rocks_options, 64ULL * 1024ULL * 1024ULL);
        rocksdb_options_set_target_file_size_base(rocks_options, 64ULL * 1024ULL * 1024ULL);
        rocksdb_options_set_max_background_jobs(rocks_options, 4);
    }

    if (rocks_read_options == NULL)
        rocks_read_options = rocksdb_readoptions_create();

    if (rocks_write_options == NULL)
        rocks_write_options = rocksdb_writeoptions_create();

    configure_write_options();

    /* Open database while holding the coordination lock for this transaction. */
    sub_start = GetCurrentTimestamp();
    if (requested_mode == ROCKS_SESSION_WRITE)
    {
        rocks_db = rocksdb_open(rocks_options, "postgresrocks_data", &err);
    }
    else
    {
        rocks_db = rocksdb_open_for_read_only(rocks_options, "postgresrocks_data", 0, &err);
    }
    postgresrocks_init_db_open_us += TimestampDifferenceMicroseconds(sub_start, GetCurrentTimestamp());

    if (err != NULL) {
        if (rocks_lock_held)
        {
            flock(rocks_lock_fd, LOCK_UN);
            rocks_lock_held = false;
        }
        rocks_session_mode = ROCKS_SESSION_NONE;
        elog(ERROR, "Failed to open RocksDB in %s mode (shared_writer=%s): %s",
             requested_mode == ROCKS_SESSION_WRITE ? "write" : "read",
             postgresrocks_shared_writer_available() ? "on" : "off",
             err);
        free(err);
    }

    rocks_session_mode = requested_mode;

    /* Initialize speculative insertions tracking */
    init_speculative_hash();
    if (!rocks_exit_callback_registered)
    {
        before_shmem_exit(postgresrocks_backend_exit, 0);
        rocks_exit_callback_registered = true;
    }
    postgresrocks_init_rocksdb_calls++;
    postgresrocks_init_rocksdb_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
}

PGDLLEXPORT void
postgresrocks_writer_main(Datum main_arg)
{
    SharedWriterQueue *queue = postgresrocks_writer_queue;
    int idle_cycles = 0;
    int ready_slots[POSTGRESROCKS_WRITER_QUEUE_SLOTS];
    int ready_counts[POSTGRESROCKS_WRITER_QUEUE_SLOTS];
    uint64 ready_seqnos[POSTGRESROCKS_WRITER_QUEUE_SLOTS];

    BackgroundWorkerUnblockSignals();

    if (queue == NULL)
        proc_exit(1);

    OwnLatch(&queue->worker_latch);

    postgresrocks_writer_pid = MyProcPid;

    for (;;)
    {
        int rc;
        int ready_count = 0;
        int i;
        char *err = NULL;
        rocksdb_writebatch_t *batch = NULL;

        ResetLatch(&queue->worker_latch);

        LWLockAcquire(postgresrocks_writer_lock, LW_SHARED);
        for (i = 0; i < POSTGRESROCKS_WRITER_QUEUE_SLOTS; i++)
        {
            if (queue->slots[i].state == WRITER_SLOT_READY)
            {
                ready_slots[ready_count] = i;
                ready_counts[ready_count] = queue->slots[i].op_count;
                ready_seqnos[ready_count] = queue->slots[i].seqno;
                ready_count++;
            }
        }

        if (ready_count > 0)
        {
            int slot_pos;

            idle_cycles = 0;
            if (rocks_db == NULL)
            {
                rocks_options = rocksdb_options_create();
                rocksdb_options_set_create_if_missing(rocks_options, 1);
                rocksdb_options_set_compression(rocks_options, rocksdb_no_compression);
                rocksdb_options_increase_parallelism(rocks_options, 4);
                rocksdb_options_optimize_level_style_compaction(rocks_options, 128ULL * 1024ULL * 1024ULL);
                rocksdb_options_set_write_buffer_size(rocks_options, 64ULL * 1024ULL * 1024ULL);
                rocksdb_options_set_target_file_size_base(rocks_options, 64ULL * 1024ULL * 1024ULL);
                rocksdb_options_set_max_background_jobs(rocks_options, 4);
                rocks_write_options = rocksdb_writeoptions_create();
                rocks_flush_options = rocksdb_flushoptions_create();
                rocksdb_flushoptions_set_wait(rocks_flush_options, 1);
                configure_write_options();
                rocks_db = rocksdb_open(rocks_options, "postgresrocks_data", &err);
                if (err != NULL || rocks_db == NULL)
                {
                    if (err != NULL)
                    {
                        elog(WARNING, "postgresrocks shared writer failed to open RocksDB: %s", err);
                        free(err);
                        err = NULL;
                    }
                    LWLockRelease(postgresrocks_writer_lock);
                    if (batch != NULL)
                        rocksdb_writebatch_destroy(batch);
                    pg_usleep(100000L);
                    continue;
                }
            }

            /* Process ready slots in sequence order. */
            for (i = 0; i < ready_count - 1; i++)
            {
                int j;

                for (j = i + 1; j < ready_count; j++)
                {
                    if (ready_seqnos[j] < ready_seqnos[i])
                    {
                        int tmp_slot = ready_slots[i];
                        int tmp_count = ready_counts[i];
                        uint64 tmp_seq = ready_seqnos[i];

                        ready_slots[i] = ready_slots[j];
                        ready_counts[i] = ready_counts[j];
                        ready_seqnos[i] = ready_seqnos[j];
                        ready_slots[j] = tmp_slot;
                        ready_counts[j] = tmp_count;
                        ready_seqnos[j] = tmp_seq;
                    }
                }
            }
            LWLockRelease(postgresrocks_writer_lock);

            batch = rocksdb_writebatch_create();
            for (slot_pos = 0; slot_pos < ready_count; slot_pos++)
            {
                SharedWriterSlot *slot = &queue->slots[ready_slots[slot_pos]];

                for (i = 0; i < ready_counts[slot_pos]; i++)
                {
                    rocksdb_writebatch_put(batch,
                                           slot->ops[i].key,
                                           slot->ops[i].key_len,
                                           slot->ops[i].value,
                                           slot->ops[i].value_len);
                }
            }

            rocksdb_write(rocks_db, rocks_write_options, batch, &err);
            rocksdb_writebatch_destroy(batch);

            if (err == NULL && rocks_flush_options != NULL)
                rocksdb_flush(rocks_db, rocks_flush_options, &err);

            LWLockAcquire(postgresrocks_writer_lock, LW_EXCLUSIVE);
            if (err != NULL)
            {
                queue->failed_seqno = ready_seqnos[0];
                strlcpy(queue->error_message, err, sizeof(queue->error_message));
                free(err);
                for (slot_pos = 0; slot_pos < ready_count; slot_pos++)
                {
                    SharedWriterSlot *slot = &queue->slots[ready_slots[slot_pos]];
                    slot->state = WRITER_SLOT_IDLE;
                    slot->seqno = 0;
                    slot->op_count = 0;
                }
            }
            else
            {
                queue->completed_seqno = ready_seqnos[ready_count - 1];
                for (slot_pos = 0; slot_pos < ready_count; slot_pos++)
                {
                    SharedWriterSlot *slot = &queue->slots[ready_slots[slot_pos]];
                    slot->state = WRITER_SLOT_IDLE;
                    slot->seqno = 0;
                    slot->op_count = 0;
                }
            }
            LWLockRelease(postgresrocks_writer_lock);
            ConditionVariableBroadcast(&queue->completion_cv);
            continue;
        }
        LWLockRelease(postgresrocks_writer_lock);

        rc = WaitLatch(&queue->worker_latch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       100L,
                       0);
        if (rc & WL_POSTMASTER_DEATH)
            break;

        if ((rc & WL_TIMEOUT) && rocks_db != NULL)
        {
            idle_cycles++;
            if (idle_cycles >= 5)
            {
                rocksdb_close(rocks_db);
                rocks_db = NULL;
                if (rocks_flush_options != NULL)
                {
                    rocksdb_flushoptions_destroy(rocks_flush_options);
                    rocks_flush_options = NULL;
                }
                idle_cycles = 0;
            }
        }
    }

    if (rocks_db != NULL)
        rocksdb_close(rocks_db);
    rocks_db = NULL;
    if (rocks_flush_options != NULL)
    {
        rocksdb_flushoptions_destroy(rocks_flush_options);
        rocks_flush_options = NULL;
    }
    DisownLatch(&queue->worker_latch);
    proc_exit(0);
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

static void
postgresrocks_backend_exit(int code, Datum arg)
{
    close_rocksdb_session();
}

/* Get or create table metadata */
static TableMetadata*
get_table_metadata_from_storage(Oid table_oid, bool create_if_missing)
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
        postgresrocks_fill_metadata_storage_id(table_oid, meta);
        persist_table_metadata_sync(table_oid, meta);
    } else {
        /* Return NULL if not found and not creating */
        pfree(meta);
        meta = NULL;
    }
    
    pfree(key);
    return meta;
}

static TableMetadata*
get_table_metadata(Oid table_oid, bool create_if_missing)
{
    TableMetadata *meta;

    meta = get_table_metadata_from_storage(table_oid, create_if_missing);
    if (meta == NULL)
        return NULL;

    (void) postgresrocks_shared_table_state_get_metadata(table_oid, meta);

    return meta;
}

/* Update table metadata */
static void
update_table_metadata(Oid table_oid, TableMetadata *meta)
{
    postgresrocks_fill_metadata_storage_id(table_oid, meta);
    persist_table_metadata_sync(table_oid, meta);
}

static void
persist_table_metadata_sync(Oid table_oid, TableMetadata *meta)
{
    char *key, *err = NULL;
    size_t key_len;

    postgresrocks_fill_metadata_storage_id(table_oid, meta);

    if (postgresrocks_shared_writer_available())
    {
        submit_metadata_to_shared_writer(table_oid, meta);
        postgresrocks_shared_table_state_write_meta(table_oid, meta);
        return;
    }

    key = make_metadata_key(table_oid, &key_len);

    if (rocks_db == NULL)
        init_rocksdb(ROCKS_SESSION_WRITE);

    rocksdb_put(rocks_db, rocks_write_options, key, key_len,
                (char*)meta, sizeof(TableMetadata), &err);

    if (err != NULL) {
        elog(ERROR, "RocksDB put error: %s", err);
        free(err);
    }

    postgresrocks_shared_table_state_write_meta(table_oid, meta);
    
    pfree(key);
}

/* Global row counter for batch operations */
static HTAB* row_counters = NULL;

/* Global write batch for accumulating single inserts */
static rocksdb_writebatch_t* global_batch = NULL;
static int batch_size = 0;
#define MAX_BATCH_SIZE 65536
#define ROWID_RESERVATION_SIZE 65536

typedef struct RowCounterEntry
{
    Oid table_oid;          /* Hash key */
    uint64 next_rowid;      /* Next row ID to assign from reserved range */
    uint64 reserved_end;    /* Last reserved row ID in current range */
    uint64 inserted_count;  /* Number of row IDs actually consumed in this xact */
    bool dirty;             /* Whether metadata needs updating */
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

static void
reserve_rowid_range(RowCounterEntry *entry, uint64 block_size)
{
    uint64 start_rowid;

    start_rowid = postgresrocks_shared_table_state_reserve_rowids(entry->table_oid,
                                                                  block_size,
                                                                  true);
    if (start_rowid == 0)
    {
        TableMetadata *meta;

        meta = get_table_metadata_from_storage(entry->table_oid, true);
        if (!meta)
            elog(ERROR, "Failed to get table metadata for table %u", entry->table_oid);

        if (meta->next_rowid == 0)
            meta->next_rowid = meta->row_count + 1;

        start_rowid = meta->next_rowid;
        meta->next_rowid += block_size;
        persist_table_metadata_sync(entry->table_oid, meta);
        pfree(meta);
    }

    entry->next_rowid = start_rowid;
    entry->reserved_end = start_rowid + block_size - 1;
}

static void
initialize_row_counter_entry(RowCounterEntry *entry)
{
    TableMetadata *meta;
    TableMetadata new_meta;
    uint64 start_rowid;

    start_rowid = postgresrocks_shared_table_state_reserve_rowids(entry->table_oid,
                                                                  ROWID_RESERVATION_SIZE,
                                                                  true);

    if (start_rowid != 0)
    {
        entry->next_rowid = start_rowid;
        entry->reserved_end = start_rowid + ROWID_RESERVATION_SIZE - 1;
        entry->inserted_count = 0;
        entry->dirty = false;
        return;
    }

    meta = get_table_metadata_from_storage(entry->table_oid, false);

    if (meta != NULL)
    {
        if (meta->next_rowid == 0)
            meta->next_rowid = meta->row_count + 1;

        start_rowid = meta->next_rowid;
        meta->next_rowid += ROWID_RESERVATION_SIZE;
        persist_table_metadata_sync(entry->table_oid, meta);
        pfree(meta);
    }
    else
    {
        MemSet(&new_meta, 0, sizeof(new_meta));
        start_rowid = 1;
        new_meta.next_rowid = start_rowid + ROWID_RESERVATION_SIZE;
        persist_table_metadata_sync(entry->table_oid, &new_meta);
    }

    entry->next_rowid = start_rowid;
    entry->reserved_end = start_rowid + ROWID_RESERVATION_SIZE - 1;
    entry->inserted_count = 0;
    entry->dirty = false;
}

/* Get next row ID for insertion (optimized for bulk operations) */
static uint64
get_next_row_id(Oid table_oid)
{
    RowCounterEntry *entry;
    bool found;
    uint64 rowid;
    TimestampTz start_ts = GetCurrentTimestamp();
    
    init_row_counter_hash();
    
    /* Look up cached counter */
    entry = (RowCounterEntry *) hash_search(row_counters,
                                           &table_oid,
                                           HASH_ENTER,
                                           &found);
    
    if (!found) {
        entry->table_oid = table_oid;
        initialize_row_counter_entry(entry);
        PR_LOG("Initialized row counter: table_oid=%u, next_rowid=%llu, reserved_end=%llu",
             table_oid,
             (unsigned long long) entry->next_rowid,
             (unsigned long long) entry->reserved_end);
    } else {
        PR_LOG("Found existing row counter: table_oid=%u, next_rowid=%llu, reserved_end=%llu, dirty=%s",
             table_oid, (unsigned long long) entry->next_rowid,
             (unsigned long long) entry->reserved_end,
             entry->dirty ? "true" : "false");
    }

    if (entry->next_rowid > entry->reserved_end)
        reserve_rowid_range(entry, ROWID_RESERVATION_SIZE);

    rowid = entry->next_rowid++;
    entry->inserted_count++;
    entry->dirty = true;
    
    PR_LOG("Assigned row ID %llu for table %u (next will be %llu)",
         (unsigned long long) rowid, table_oid,
         (unsigned long long) entry->next_rowid);
    postgresrocks_rowid_calls++;
    postgresrocks_rowid_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
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
clear_row_counter_entry(Oid table_oid)
{
    if (row_counters == NULL)
        return;

    hash_search(row_counters, &table_oid, HASH_REMOVE, NULL);
}

static void
flush_write_batch(void)
{
    char *err = NULL;
    TimestampTz start_ts;

    if (pending_write_count > 0)
    {
        int rows = batch_size;

        start_ts = GetCurrentTimestamp();
        flush_pending_writes_to_shared_writer();
        postgresrocks_batch_flush_calls++;
        postgresrocks_batch_flush_rows += rows;
        postgresrocks_batch_flush_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
        batch_size = 0;
        return;
    }

    if (global_batch == NULL || batch_size == 0) {
        PR_LOG("flush_write_batch called but no batch to flush");
        return;
    }

    if (!postgresrocks_shared_writer_available())
        init_rocksdb(ROCKS_SESSION_WRITE);
    PR_LOG("About to flush write batch with %d operations", batch_size);
    start_ts = GetCurrentTimestamp();

    /* Execute the accumulated batch */
    rocksdb_write(rocks_db, rocks_write_options, global_batch, &err);

    if (err != NULL) {
        elog(ERROR, "RocksDB batch write error: %s", err);
        free(err);
        return;
    }

    PR_LOG("Successfully flushed write batch with %d accumulated operations", batch_size);
    postgresrocks_batch_flush_calls++;
    postgresrocks_batch_flush_rows += batch_size;
    postgresrocks_batch_flush_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());

    /* Reset batch */
    rocksdb_writebatch_clear(global_batch);
    batch_size = 0;
}

static uint64
delete_table_storage_for_identity(Oid table_oid, Oid storage_id, bool delete_metadata)
{
    char *prefix;
    char *meta_key = NULL;
    char *err = NULL;
    size_t prefix_len;
    size_t meta_key_len = 0;
    rocksdb_iterator_t *iter;
    rocksdb_writebatch_t *batch;
    uint64 deleted_rows = 0;
    bool touched_batch = false;

    if (postgresrocks_shared_writer_available())
    {
        ensure_writes_flushed();
        close_rocksdb_session();
        pg_usleep(600000L);
    }

    init_rocksdb(ROCKS_SESSION_WRITE);

    prefix = NULL;
    iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    batch = rocksdb_writebatch_create();

    if (OidIsValid(storage_id))
    {
        prefix = make_row_prefix(table_oid, storage_id, &prefix_len);
        rocksdb_iter_seek(iter, prefix, prefix_len);

        while (rocksdb_iter_valid(iter))
        {
            const char *key;
            size_t key_len;

            key = rocksdb_iter_key(iter, &key_len);

            if (key_len < prefix_len || memcmp(key, prefix, prefix_len) != 0)
                break;

            rocksdb_writebatch_delete(batch, key, key_len);
            deleted_rows++;
            touched_batch = true;
            rocksdb_iter_next(iter);
        }
    }

    if (delete_metadata)
    {
        meta_key = make_metadata_key(table_oid, &meta_key_len);
        rocksdb_writebatch_delete(batch, meta_key, meta_key_len);
        touched_batch = true;
        postgresrocks_shared_table_state_clear(table_oid);
    }
    else
    {
        TableMetadata *meta = get_table_metadata(table_oid, false);

        if (meta != NULL)
        {
            meta->row_count = 0;
            meta->next_rowid = 1;
            meta_key = make_metadata_key(table_oid, &meta_key_len);
            rocksdb_writebatch_put(batch,
                                   meta_key, meta_key_len,
                                   (const char *) meta, sizeof(TableMetadata));
            touched_batch = true;
            postgresrocks_shared_table_state_write(table_oid, meta->row_count, meta->next_rowid);
            pfree(meta);
        }
    }

    if (touched_batch)
    {
        rocksdb_write(rocks_db, rocks_write_options, batch, &err);
        if (err != NULL)
        {
            elog(ERROR, "RocksDB batch delete error: %s", err);
            free(err);
        }
    }

    rocksdb_writebatch_destroy(batch);
    rocksdb_iter_destroy(iter);
    if (meta_key != NULL)
        pfree(meta_key);
    if (prefix != NULL)
        pfree(prefix);

    clear_row_counter_entry(table_oid);

    return deleted_rows;
}

static uint64
delete_table_storage_internal(Oid table_oid, bool delete_metadata)
{
    Oid storage_id = postgresrocks_lookup_storage_id_for_cleanup(table_oid);

    return delete_table_storage_for_identity(table_oid, storage_id, delete_metadata);
}

static void flush_row_counter_metadata_internal(Oid table_oid, bool flush_rows_first, bool defer_metadata_write);
static void flush_row_counter_metadata(Oid table_oid);

/* Force flush of any pending writes - called from scan operations */
static void
ensure_writes_flushed(void)
{
    bool flushed_metadata = false;

    if ((global_batch != NULL && batch_size > 0) || pending_write_count > 0) {
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
                flushed_metadata = true;
            }
        }
    }

    if (flushed_metadata)
        flush_write_batch();
}

/* Flush row counter metadata to storage */
static void
flush_row_counter_metadata_internal(Oid table_oid, bool flush_rows_first, bool defer_metadata_write)
{
    RowCounterEntry *entry;
    bool found;
    TableMetadata *meta;
    char *meta_key = NULL;
    size_t meta_key_len = 0;
    TimestampTz start_ts;
    
    PR_LOG("flush_row_counter_metadata called for table_oid=%u", table_oid);
    start_ts = GetCurrentTimestamp();
    
    /* First flush any pending writes when the caller requires it. */
    if (flush_rows_first)
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
    
    meta = palloc0(sizeof(TableMetadata));
    if (!postgresrocks_shared_table_state_get_metadata(table_oid, meta))
    {
        pfree(meta);
        meta = get_table_metadata(table_oid, true);
    }
    if (meta) {
        if (meta->next_rowid == 0)
            meta->next_rowid = meta->row_count + 1;
        meta->row_count += entry->inserted_count;
        if (meta->next_rowid < entry->reserved_end + 1)
            meta->next_rowid = entry->reserved_end + 1;
        if (defer_metadata_write && postgresrocks_shared_writer_available())
        {
            meta_key = make_metadata_key(table_oid, &meta_key_len);
            queue_pending_write(meta_key, (uint16) meta_key_len, (const char *) meta, sizeof(TableMetadata));
            postgresrocks_shared_table_state_write_meta(table_oid, meta);
            pfree(meta_key);
        }
        else
        {
            update_table_metadata(table_oid, meta);
        }
        entry->dirty = false;
        entry->inserted_count = 0;
        postgresrocks_metadata_flush_calls++;
        postgresrocks_metadata_flush_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
        PR_LOG("Successfully flushed metadata: table_oid=%u, row_count=%llu",
             table_oid, (unsigned long long) meta->row_count);
        pfree(meta);
    } else {
        elog(ERROR, "Failed to get table metadata for flushing");
    }
}

static void
flush_row_counter_metadata(Oid table_oid)
{
    flush_row_counter_metadata_internal(table_oid, true, false);
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
                entry->next_rowid = meta->next_rowid == 0 ? meta->row_count + 1 : meta->next_rowid;
                entry->reserved_end = entry->next_rowid - 1;
                pfree(meta);
            } else {
                entry->next_rowid = 1;
                entry->reserved_end = 0;
            }
            entry->inserted_count = 0;
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
            if (rocks_db != NULL || pending_write_count > 0 || (row_counters != NULL && hash_get_num_entries(row_counters) > 0)) {
                HASH_SEQ_STATUS status;
                RowCounterEntry *entry;
                TimestampTz start_ts = GetCurrentTimestamp();

                if (row_counters != NULL) {
                    hash_seq_init(&status, row_counters);
                    while ((entry = hash_seq_search(&status)) != NULL) {
                        if (entry->dirty)
                            flush_row_counter_metadata_internal(entry->table_oid,
                                                               !postgresrocks_shared_writer_available(),
                                                               postgresrocks_shared_writer_available());
                    }
                }

                flush_write_batch();

                clear_row_counter_cache();
                clear_row_read_cache();
                close_rocksdb_session();
                postgresrocks_precommit_flush_calls++;
                postgresrocks_precommit_flush_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
            }
            break;

        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            if (global_batch != NULL)
                rocksdb_writebatch_clear(global_batch);
            batch_size = 0;
            reset_pending_writes();
            reset_dirty_row_counters();
            clear_row_counter_cache();
            clear_row_read_cache();
            close_rocksdb_session();
            break;

        case XACT_EVENT_COMMIT:
        case XACT_EVENT_PARALLEL_COMMIT:
            clear_row_counter_cache();
            clear_row_read_cache();
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
static IndexFetchTableData *rocks_index_fetch_begin(Relation rel);
static void rocks_index_fetch_reset(IndexFetchTableData *data);
static void rocks_index_fetch_end(IndexFetchTableData *data);
static bool rocks_index_fetch_tuple(IndexFetchTableData *scan,
                                    ItemPointer tid,
                                    Snapshot snapshot,
                                    TupleTableSlot *slot,
                                    bool *call_again,
                                    bool *all_dead);
static bool rocks_tuple_satisfies_snapshot(Relation rel,
                                           TupleTableSlot *slot,
                                           Snapshot snapshot);
static bool rocks_tuple_fetch_row_version(Relation rel,
                                          ItemPointer tid,
                                          Snapshot snapshot,
                                          TupleTableSlot *slot);
static bool rocks_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static void rocks_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid);
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
    .index_fetch_begin = rocks_index_fetch_begin,
    .index_fetch_reset = rocks_index_fetch_reset,
    .index_fetch_end = rocks_index_fetch_end,
    .index_fetch_tuple = rocks_index_fetch_tuple,

    /* Non-modifying operations on individual tuples */
    .tuple_fetch_row_version = rocks_tuple_fetch_row_version,
    .tuple_tid_valid = rocks_tuple_tid_valid,
    .tuple_get_latest_tid = rocks_tuple_get_latest_tid,
    .tuple_satisfies_snapshot = rocks_tuple_satisfies_snapshot,
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

        table_oid = resolve_table_oid_by_name(table_name);
        
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
        unsigned char *null_bitmap;
        char *data_ptr;
        size_t nullmap_size;
        
        /* Get row data */
        row_key = make_row_key(table_oid, rocks_storage_id_by_table_oid(table_oid), rowid, &row_key_len);
        row_data = rocksdb_get(rocks_db, rocks_read_options, row_key, row_key_len, &row_data_len, &err);
        
        if (err != NULL || row_data == NULL) {
            pfree(row_key);
            if (err) free(err);
            SRF_RETURN_DONE(funcctx);
        }
        
        /* Parse row header */
        header = (RowHeader *) row_data;
        nullmap_size = NULL_BITMAP_SIZE(header->natts);
        null_bitmap = (unsigned char *) (row_data + sizeof(RowHeader));
        data_ptr = row_data + sizeof(RowHeader) + nullmap_size;
        
        /* Build a tuple with actual data */
        values = (char **) palloc(2 * sizeof(char *));
        
        /* Extract ID (column 0) */
        if (row_attr_is_null(null_bitmap, 0)) {
            values[0] = pstrdup("NULL");
        } else {
            int32 id_value;
            memcpy(&id_value, data_ptr, sizeof(int32));
            values[0] = psprintf("%d", id_value);
            data_ptr += sizeof(int32);
        }
        
        /* Extract name (column 1) */
        if (header->natts > 1) {
            if (row_attr_is_null(null_bitmap, 1)) {
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

PG_FUNCTION_INFO_V1(postgresrocks_count_rows);
Datum
postgresrocks_count_rows(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_P(0);
    char *table_name = text_to_cstring(table_name_text);
    Oid table_oid = resolve_table_oid_by_name(table_name);
    TableMetadata *meta;
    uint64 row_count = 0;

    if (table_oid == InvalidOid)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("table \"%s\" does not exist", table_name)));
    }

    if (postgresrocks_shared_table_state_read(table_oid, &row_count, NULL))
        PG_RETURN_INT64((int64) row_count);

    ensure_writes_flushed();
    init_rocksdb(ROCKS_SESSION_READ);

    meta = get_table_metadata(table_oid, false);
    if (meta != NULL)
    {
        row_count = meta->row_count;
        pfree(meta);
    }

    PG_RETURN_INT64((int64) row_count);
}

PG_FUNCTION_INFO_V1(postgresrocks_lookup_text);
Datum
postgresrocks_lookup_text(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_P(0);
    int64 rowid = PG_GETARG_INT64(1);
    int32 attnum = PG_GETARG_INT32(2);
    char *table_name = text_to_cstring(table_name_text);
    Oid table_oid = resolve_table_oid_by_name(table_name);
    char *row_data;
    size_t row_data_len;
    int target_index = attnum - 1;
    RowEncodingCacheEntry *cache;
    text *result_text;

    if (table_oid == InvalidOid)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("table \"%s\" does not exist", table_name)));
    }

    if (attnum <= 0)
        ereport(ERROR, (errmsg("attribute number must be positive")));

    ensure_writes_flushed();
    init_rocksdb(ROCKS_SESSION_READ);

    row_data = fetch_cached_row_data_by_rowid(table_oid, (uint64) rowid, &row_data_len);
    if (row_data == NULL)
        PG_RETURN_NULL();

    cache = get_row_encoding_cache_by_oid(table_oid);

    if (target_index >= cache->natts)
    {
        ereport(ERROR,
                (errmsg("attribute number %d is out of range for table \"%s\"",
                        attnum,
                        table_name)));
    }

    if (cache->codecs[target_index].kind != ATTR_CODEC_TEXT)
    {
        ereport(ERROR,
                (errmsg("attribute %d of table \"%s\" is not text-compatible",
                        attnum,
                        table_name)));
    }

    result_text = decode_text_attribute_from_row(row_data, cache, target_index);
    if (result_text == NULL)
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(result_text);
}

PG_FUNCTION_INFO_V1(postgresrocks_reset_read_cache);
Datum
postgresrocks_reset_read_cache(PG_FUNCTION_ARGS)
{
    clear_row_read_cache();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(postgresrocks_handle_sql_drop);
Datum
postgresrocks_handle_sql_drop(PG_FUNCTION_ARGS)
{
    int spi_rc;
    uint64 i;

    spi_rc = SPI_connect();
    if (spi_rc != SPI_OK_CONNECT)
        elog(ERROR, "postgresrocks_handle_sql_drop: SPI_connect failed: %d", spi_rc);

    spi_rc = SPI_execute(
        "SELECT objid "
        "FROM pg_event_trigger_dropped_objects() "
        "WHERE classid = 'pg_class'::regclass "
        "  AND object_type = 'table'",
        true,
        0);
    if (spi_rc != SPI_OK_SELECT)
        elog(ERROR, "postgresrocks_handle_sql_drop: SPI_execute failed: %d", spi_rc);

    for (i = 0; i < SPI_processed; i++)
    {
        bool isnull = false;
        Oid table_oid;
        TableMetadata *meta;

        table_oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
                                                   SPI_tuptable->tupdesc,
                                                   1,
                                                   &isnull));
        if (isnull || !OidIsValid(table_oid))
            continue;

        meta = get_table_metadata_from_storage(table_oid, false);
        if (meta == NULL)
            continue;
        pfree(meta);

        delete_table_storage_internal(table_oid, true);
    }

    SPI_finish();
    PG_RETURN_NULL();
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
    postgresrocks_reset_direct_stats_internal();

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

PG_FUNCTION_INFO_V1(postgresrocks_direct_write_stats);
Datum
postgresrocks_direct_write_stats(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[20];
    bool nulls[20] = {false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false};

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    values[0] = Int64GetDatum((int64) postgresrocks_serialize_calls);
    values[1] = Int64GetDatum((int64) postgresrocks_serialize_us);
    values[2] = Int64GetDatum((int64) postgresrocks_batch_flush_calls);
    values[3] = Int64GetDatum((int64) postgresrocks_batch_flush_rows);
    values[4] = Int64GetDatum((int64) postgresrocks_batch_flush_us);
    values[5] = Int64GetDatum((int64) postgresrocks_metadata_flush_calls);
    values[6] = Int64GetDatum((int64) postgresrocks_metadata_flush_us);
    values[7] = Int64GetDatum((int64) postgresrocks_rowid_calls);
    values[8] = Int64GetDatum((int64) postgresrocks_rowid_us);
    values[9] = Int64GetDatum((int64) postgresrocks_batch_put_calls);
    values[10] = Int64GetDatum((int64) postgresrocks_batch_put_us);
    values[11] = Int64GetDatum((int64) postgresrocks_tuple_insert_us);
    values[12] = Int64GetDatum((int64) postgresrocks_init_rocksdb_calls);
    values[13] = Int64GetDatum((int64) postgresrocks_init_rocksdb_us);
    values[14] = Int64GetDatum((int64) postgresrocks_precommit_flush_us);
    values[15] = Int64GetDatum((int64) postgresrocks_init_lockfile_open_us);
    values[16] = Int64GetDatum((int64) postgresrocks_init_flock_us);
    values[17] = Int64GetDatum((int64) postgresrocks_init_db_open_us);
    values[18] = Int64GetDatum((int64) postgresrocks_init_upgrade_reopen_calls);
    values[19] = Int64GetDatum((int64) postgresrocks_init_upgrade_reopen_us);

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
    PR_LOG("rocks_slot_callbacks returning TTSOpsPostgresRocksLazy");
    return &TTSOpsPostgresRocksLazy;
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
    scan->key_prefix = make_row_prefix(table_oid, rocks_relation_storage_id(relation), &scan->key_prefix_len);
    scan->iter = rocksdb_create_iterator(rocks_db, rocks_read_options);
    scan->encoding_cache = get_row_encoding_cache(relation);
    scan->current_rowid = 1;  /* Start from first row */
    scan->started = false;
    scan->analyze_block_reported = false;
    rocks_init_fast_filters(scan, relation, nkeys, key);
    
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
    RocksScanDesc *scan = (RocksScanDesc *) sscan;

    scan->current_rowid = 1;
    scan->started = false;
    scan->analyze_block_reported = false;
    if (scan->fast_filters != NULL)
    {
        pfree(scan->fast_filters);
        scan->fast_filters = NULL;
        scan->fast_filter_count = 0;
    }
    if (key != NULL)
        scan->rs_base.rs_key = key;
    rocks_init_fast_filters(scan, scan->rs_base.rs_rd, scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
    PR_LOG("rocks_rescan called");
}

static IndexFetchTableData *
rocks_index_fetch_begin(Relation rel)
{
    RocksIndexFetchData *fetch = palloc0(sizeof(RocksIndexFetchData));

    fetch->base.rel = rel;
    ensure_writes_flushed();
    init_rocksdb(ROCKS_SESSION_READ);
    fetch->cache = get_row_encoding_cache(rel);
    fetch->prepared = true;
    return &fetch->base;
}

static void
rocks_index_fetch_reset(IndexFetchTableData *data)
{
    (void) data;
}

static void
rocks_index_fetch_end(IndexFetchTableData *data)
{
    pfree(data);
}

static bool
rocks_tuple_fetch_row_version(Relation rel,
                              ItemPointer tid,
                              Snapshot snapshot,
                              TupleTableSlot *slot)
{
    Oid table_oid = RelationGetRelid(rel);
    uint64 rowid = tid_to_rowid(tid);
    size_t row_data_len;
    char *row_data;
    RowEncodingCacheEntry *cache;

    (void) snapshot;
    ensure_writes_flushed();
    init_rocksdb(ROCKS_SESSION_READ);

    row_data = fetch_cached_row_data_by_rowid(table_oid, rowid, &row_data_len);
    if (row_data == NULL)
        return false;

    cache = get_row_encoding_cache(rel);
    rocks_store_borrowed_row(slot, row_data, row_data_len, cache, rowid);
    return true;
}

static bool
rocks_index_fetch_tuple(IndexFetchTableData *scan,
                        ItemPointer tid,
                        Snapshot snapshot,
                        TupleTableSlot *slot,
                        bool *call_again,
                        bool *all_dead)
{
    RocksIndexFetchData *fetch = (RocksIndexFetchData *) scan;
    Oid table_oid = RelationGetRelid(scan->rel);
    uint64 rowid = tid_to_rowid(tid);
    size_t row_data_len;
    const char *row_data;
    bool found;

    if (call_again != NULL)
        *call_again = false;
    if (all_dead != NULL)
        *all_dead = false;

    (void) snapshot;

    if (!fetch->prepared)
    {
        ensure_writes_flushed();
        init_rocksdb(ROCKS_SESSION_READ);
        fetch->cache = get_row_encoding_cache(scan->rel);
        fetch->prepared = true;
    }

    row_data = fetch_cached_row_data_by_rowid(table_oid, rowid, &row_data_len);
    found = (row_data != NULL);
    if (found)
        rocks_store_borrowed_row(slot, row_data, row_data_len, fetch->cache, rowid);
    if (!found)
        ExecClearTuple(slot);
    return found;
}

static bool
rocks_tuple_satisfies_snapshot(Relation rel,
                               TupleTableSlot *slot,
                               Snapshot snapshot)
{
    (void) rel;
    (void) slot;
    (void) snapshot;
    return true;
}

static bool
rocks_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    TableMetadata *meta;
    uint64 rowid;
    bool valid = false;

    meta = get_table_metadata(RelationGetRelid(scan->rs_rd), false);
    if (meta == NULL)
        return false;

    rowid = tid_to_rowid(tid);
    valid = rowid >= 1 && rowid <= meta->row_count;
    pfree(meta);
    return valid;
}

static void
rocks_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
    (void) scan;
    (void) tid;
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
    uint64 rowid;
    
    PR_LOG("rocks_getnextslot called - current_rowid=%llu, total_rows=%llu",
         (unsigned long long) scan->current_rowid,
         (unsigned long long) scan->total_rows);
    
    if (!scan->started) {
        rocksdb_iter_seek(scan->iter, scan->key_prefix, scan->key_prefix_len);
        scan->started = true;
    }

    for (;;)
    {
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
        rowid = decode_u64_be((const unsigned char *) key + 1 + sizeof(uint32) + sizeof(uint32));

        if (scan->fast_filter_count > 0 &&
            !rocks_row_matches_fast_filters(row_data, scan))
        {
            scan->current_rowid++;
            rocksdb_iter_next(scan->iter);
            continue;
        }

        PR_LOG("rocks_getnextslot - found row %llu, data_len=%zu",
             (unsigned long long) rowid, row_data_len);

        /* Store the raw row and decode attributes lazily on demand */
        rocks_store_lazy_row(slot, row_data, row_data_len, scan->encoding_cache, rowid);
        scan->current_rowid++;
        rocksdb_iter_next(scan->iter);

        PR_LOG("rocks_getnextslot - successfully deserialized row %llu",
             (unsigned long long) (scan->current_rowid - 1));
        return true;
    }
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
        if (scan->fast_filters) {
            pfree(scan->fast_filters);
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
    RowEncodingCacheEntry *encoding_cache;
    uint64 rowid;
    unsigned char row_key[ROW_KEY_LEN];
    char *row_data;
    size_t row_data_len;
    bool use_shared_writer = postgresrocks_use_shared_writer_for_relation(relation);
    TimestampTz start_ts = GetCurrentTimestamp();
    
    PR_LOG("rocks_tuple_insert - table_oid=%u", table_oid);
    postgresrocks_tuple_insert_calls++;

    /*
     * In the direct path we start write statements in write mode immediately so
     * rowid reservation and metadata access do not first open a read-only
     * RocksDB handle and then reopen under an exclusive lock. The shared writer
     * path skips direct RocksDB opens entirely.
     */
    if (!use_shared_writer)
        init_rocksdb(ROCKS_SESSION_WRITE);
    
    /* Get next row ID (cached for performance) */
    rowid = get_next_row_id(table_oid);
    encoding_cache = get_row_encoding_cache(relation);
    
    /* Serialize the complete row */
    row_data = serialize_row_reuse(slot, encoding_cache, &row_data_len);
    
    /* Create row key */
    fill_row_key(row_key, table_oid, rocks_relation_storage_id(relation), rowid);
    rowid_to_tid(rowid, &slot->tts_tid);

    if (use_shared_writer)
    {
        TimestampTz batch_put_start = GetCurrentTimestamp();

        queue_pending_write((const char *) row_key, ROW_KEY_LEN, row_data, (uint32) row_data_len);
        postgresrocks_batch_put_calls++;
        postgresrocks_batch_put_us += TimestampDifferenceMicroseconds(batch_put_start, GetCurrentTimestamp());
    }
    else
    {
        /* Initialize global batch if needed */
        if (global_batch == NULL) {
            global_batch = rocksdb_writebatch_create();
            batch_size = 0;
        }

        /* Add to batch instead of immediate write */
        {
            TimestampTz batch_put_start = GetCurrentTimestamp();

            rocksdb_writebatch_put(global_batch, (const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
            postgresrocks_batch_put_calls++;
            postgresrocks_batch_put_us += TimestampDifferenceMicroseconds(batch_put_start, GetCurrentTimestamp());
        }
    }
    batch_size++;

    /* Flush batch if it's getting large */
    if (batch_size >= MAX_BATCH_SIZE) {
        flush_write_batch();
    }
    
    PR_LOG("rocks_tuple_insert - added row %llu to batch (size=%d), data_len=%zu",
         (unsigned long long) rowid, batch_size, row_data_len);
    postgresrocks_tuple_insert_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
    
}

static void
rocks_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                              CommandId cid, int options,
                              BulkInsertState bistate,
                              uint32 specToken)
{
    Oid table_oid = RelationGetRelid(relation);
    RowEncodingCacheEntry *encoding_cache;
    uint64 rowid;
    SpeculativeInsertEntry *entry;
    bool found;

    init_rocksdb(ROCKS_SESSION_WRITE);
    
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
    entry->key = make_row_key(table_oid, rocks_relation_storage_id(relation), rowid, &entry->key_len);
    encoding_cache = get_row_encoding_cache(relation);
    
    /* Serialize the complete row data for storage */
    entry->data = serialize_row(slot, encoding_cache, &entry->data_len);
    
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
        init_rocksdb(ROCKS_SESSION_WRITE);
        rocksdb_put(rocks_db, rocks_write_options,
                   entry->key, entry->key_len,
                   entry->data, entry->data_len, &err);

        if (err != NULL) {
            elog(ERROR, "RocksDB put error during speculative completion: %s", err);
            free(err);
        }
        
        PR_LOG("Speculative insertion succeeded for row %llu",
             (unsigned long long) entry->rowid);
    } else {
        /*
         * Speculation failed - keep the reserved row ID but do not count it as
         * a committed inserted tuple in this transaction.
         */
        RowCounterEntry *counter_entry;

        table_oid = entry->table_oid;
        if (row_counters != NULL)
        {
            counter_entry = (RowCounterEntry *) hash_search(row_counters,
                                                            &table_oid,
                                                            HASH_FIND,
                                                            &found);
            if (counter_entry != NULL && counter_entry->inserted_count > 0)
                counter_entry->inserted_count--;
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
    RowEncodingCacheEntry *encoding_cache;
    rocksdb_writebatch_t* batch;
    char *err = NULL;
    int i;
    bool use_shared_writer = postgresrocks_use_shared_writer_for_relation(relation);
    
    PR_LOG("rocks_multi_insert - inserting %d tuples for table_oid=%u", ntuples, table_oid);
    postgresrocks_multi_insert_calls++;
    postgresrocks_multi_inserted_tuples += ntuples;

    if (!use_shared_writer)
        init_rocksdb(ROCKS_SESSION_WRITE);
    encoding_cache = get_row_encoding_cache(relation);
    if (!use_shared_writer)
        batch = rocksdb_writebatch_create();
    else
        batch = NULL;
    
    for (i = 0; i < ntuples; i++) {
        uint64 rowid;
        unsigned char row_key[ROW_KEY_LEN];
        char *row_data;
        size_t row_data_len;
        
        /* Get next row ID (cached, no metadata write yet) */
        rowid = get_next_row_id(table_oid);
        
        /* Serialize the complete row */
        row_data = serialize_row_reuse(slots[i], encoding_cache, &row_data_len);
        
        /* Create row key */
        fill_row_key(row_key, table_oid, rocks_relation_storage_id(relation), rowid);
        rowid_to_tid(rowid, &slots[i]->tts_tid);

        {
            TimestampTz batch_put_start = GetCurrentTimestamp();

            if (use_shared_writer)
                queue_pending_write((const char *) row_key, ROW_KEY_LEN, row_data, (uint32) row_data_len);
            else
                rocksdb_writebatch_put(batch, (const char *) row_key, ROW_KEY_LEN, row_data, row_data_len);
            postgresrocks_batch_put_calls++;
            postgresrocks_batch_put_us += TimestampDifferenceMicroseconds(batch_put_start, GetCurrentTimestamp());
        }
    }
    
    if (use_shared_writer)
    {
        batch_size += ntuples;
        flush_write_batch();
    }
    else
    {
        /* Execute the entire batch in one operation */
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
    
    /* Decode row ID from ItemPointer */
    rowid = tid_to_rowid(tid);
    
    /* Create key for this row */
    fill_row_key(key, table_oid, rocks_relation_storage_id(relation), rowid);
    
    init_rocksdb(ROCKS_SESSION_WRITE);
    /* Delete from RocksDB */
    rocksdb_delete(rocks_db, rocks_write_options, (const char *) key, ROW_KEY_LEN, &err);

    if (err != NULL) {
        elog(ERROR, "RocksDB delete error: %s", err);
        free(err);
        return TM_Deleted; /* Indicate failure */
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

    /*
     * PostgreSQL can reuse relation OIDs. Since postgresrocks keys are
     * prefixed by table OID, stale RocksDB rows for a reused OID must be
     * cleared before the new table starts using that identifier.
     */
    delete_table_storage_for_identity(table_oid, newrlocator->relNumber, true);

    /*
     * Metadata is initialized lazily on first write. Keeping CREATE TABLE
     * side effects minimal helps both the direct path and the shared-writer
     * path avoid unnecessary RocksDB opens during DDL.
     */

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

    delete_table_storage_internal(table_oid, false);
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
    RocksScanDesc *rscan = (RocksScanDesc *) scan;

    (void) stream;
    if (rscan->analyze_block_reported || rscan->total_rows == 0)
        return false;

    rscan->analyze_block_reported = true;
    return true;
}

static bool
rocks_scan_analyze_next_tuple(TableScanDesc scan,
                             TransactionId OldestXmin,
                             double *liverows, double *deadrows,
                             TupleTableSlot *slot)
{
    (void) OldestXmin;

    if (rocks_getnextslot(scan, ForwardScanDirection, slot))
    {
        *liverows += 1.0;
        return true;
    }

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
    TableScanDesc localscan = scan;
    TupleTableSlot *slot;
    double tuples = 0.0;
    Datum *values;
    bool *isnull;
    bool owns_scan = false;

    (void) allow_sync;
    (void) anyvisible;
    (void) progress;
    (void) start_blockno;
    (void) numblocks;

    if (localscan == NULL)
    {
        localscan = rocks_beginscan(tablerel,
                                    GetLatestSnapshot(),
                                    0,
                                    NULL,
                                    NULL,
                                    SO_TYPE_SEQSCAN);
        owns_scan = true;
    }

    slot = MakeSingleTupleTableSlot(RelationGetDescr(tablerel),
                                    table_slot_callbacks(tablerel));
    values = palloc(sizeof(Datum) * indexInfo->ii_NumIndexAttrs);
    isnull = palloc(sizeof(bool) * indexInfo->ii_NumIndexAttrs);

    while (rocks_getnextslot(localscan, ForwardScanDirection, slot))
    {
        FormIndexDatum(indexInfo, slot, NULL, values, isnull);
        callback(indexrel, &slot->tts_tid, values, isnull, true, callback_state);
        ExecClearTuple(slot);
        tuples += 1.0;
    }

    pfree(values);
    pfree(isnull);
    ExecDropSingleTupleTableSlot(slot);
    if (owns_scan)
        rocks_endscan(localscan);

    return tuples;
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
    prefix = make_row_prefix(table_oid, rocks_relation_storage_id(rel), &prefix_len);
    
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
    TableMetadata *meta;
    TupleDesc tupdesc = RelationGetDescr(rel);
    double tuple_count = 0.0;
    uint64 estimated_bytes = 0;
    size_t row_overhead;
    int i;

    meta = get_table_metadata(RelationGetRelid(rel), false);
    if (meta != NULL)
    {
        tuple_count = (double) meta->row_count;
        pfree(meta);
    }

    row_overhead = sizeof(RowHeader) + NULL_BITMAP_SIZE(tupdesc->natts) + ROW_KEY_LEN;
    estimated_bytes = (uint64) row_overhead;
    for (i = 0; i < tupdesc->natts; i++)
    {
        int32 width = attr_widths != NULL ? attr_widths[i] : 0;

        if (width <= 0)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

            if (attr->attlen > 0)
                width = attr->attlen;
            else
                width = 32;
        }

        estimated_bytes += (uint64) width;
    }

    *tuples = tuple_count;
    if (tuple_count <= 0.0)
        *pages = 0;
    else
    {
        uint64 total_bytes = (uint64) (tuple_count * estimated_bytes);
        uint64 estimated_pages = (total_bytes + BLCKSZ - 1) / BLCKSZ;

        if (estimated_pages == 0)
            estimated_pages = 1;
        *pages = (BlockNumber) estimated_pages;
    }
    *allvisfrac = 1.0;
}
