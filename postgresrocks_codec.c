#include "postgres.h"

#include "postgresrocks_codec.h"

#include "access/table.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

static char *serialize_buffer = NULL;
static size_t serialize_buffer_capacity = 0;
static Datum *serialize_attr_values = NULL;
static bool *serialize_attr_isnull = NULL;
static struct varlena **serialize_attr_texts = NULL;
static const char **serialize_attr_text_data = NULL;
static int32 *serialize_attr_text_lens = NULL;
static int serialize_attr_capacity = 0;
static HTAB *row_encoding_cache = NULL;

extern uint64 postgresrocks_serialize_calls;
extern uint64 postgresrocks_serialize_us;

typedef struct RowSerializeScratch
{
    Datum *values;
    bool *isnulls;
    struct varlena **texts;
    const char **text_data;
    int32 *text_lens;
} RowSerializeScratch;

static void ensure_serialize_attr_capacity(int natts);
static void prepare_text_datum(Datum value, struct varlena **text_ptr, const char **data_ptr, int32 *len);
static Size codec_measure_int4(AttrCodec *codec, Datum value, bool isnull,
                               struct varlena **text_ptr, const char **text_data, int32 *text_len);
static Size codec_measure_int8(AttrCodec *codec, Datum value, bool isnull,
                               struct varlena **text_ptr, const char **text_data, int32 *text_len);
static Size codec_measure_text(AttrCodec *codec, Datum value, bool isnull,
                               struct varlena **text_ptr, const char **text_data, int32 *text_len);
static void codec_encode_int4(AttrCodec *codec, char **dst, Datum value,
                              struct varlena *text_ptr, const char *text_data, int32 text_len);
static void codec_encode_int8(AttrCodec *codec, char **dst, Datum value,
                              struct varlena *text_ptr, const char *text_data, int32 text_len);
static void codec_encode_text(AttrCodec *codec, char **dst, Datum value,
                              struct varlena *text_ptr, const char *text_data, int32 text_len);
static Datum codec_decode_int4(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt);
static Datum codec_decode_int8(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt);
static Datum codec_decode_text(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt);
static void codec_skip_int4(AttrCodec *codec, const char **src);
static void codec_skip_int8(AttrCodec *codec, const char **src);
static void codec_skip_text(AttrCodec *codec, const char **src);
static void init_row_encoding_cache_hash(void);
static RowSerializeScratch get_row_serialize_scratch(int natts);
static size_t prepare_row_for_serialization(TupleTableSlot *slot, RowEncodingCacheEntry *cache,
                                            RowSerializeScratch *scratch);
static char *write_row_header_and_nullmap(char *data, RowEncodingCacheEntry *cache,
                                          RowSerializeScratch *scratch);
static uint32 *write_row_offset_table(char **ptr, RowEncodingCacheEntry *cache,
                                      RowSerializeScratch *scratch);
static void finalize_row_header(char *data, uint32 natts, uint32 data_len);
static void encode_serialized_attributes(char **ptr, uint32 *offsets, RowEncodingCacheEntry *cache,
                                         RowSerializeScratch *scratch);

static void
ensure_serialize_attr_capacity(int natts)
{
    if (serialize_attr_capacity >= natts)
        return;

    if (serialize_attr_values == NULL)
    {
        serialize_attr_values = MemoryContextAlloc(TopMemoryContext, sizeof(Datum) * natts);
        serialize_attr_isnull = MemoryContextAlloc(TopMemoryContext, sizeof(bool) * natts);
        serialize_attr_texts = MemoryContextAlloc(TopMemoryContext, sizeof(struct varlena *) * natts);
        serialize_attr_text_data = MemoryContextAlloc(TopMemoryContext, sizeof(char *) * natts);
        serialize_attr_text_lens = MemoryContextAlloc(TopMemoryContext, sizeof(int32) * natts);
    }
    else
    {
        serialize_attr_values = repalloc(serialize_attr_values, sizeof(Datum) * natts);
        serialize_attr_isnull = repalloc(serialize_attr_isnull, sizeof(bool) * natts);
        serialize_attr_texts = repalloc(serialize_attr_texts, sizeof(struct varlena *) * natts);
        serialize_attr_text_data = repalloc(serialize_attr_text_data, sizeof(char *) * natts);
        serialize_attr_text_lens = repalloc(serialize_attr_text_lens, sizeof(int32) * natts);
    }

    serialize_attr_capacity = natts;
}

static void
prepare_text_datum(Datum value, struct varlena **text_ptr, const char **data_ptr, int32 *len)
{
    struct varlena *txt = (struct varlena *) DatumGetPointer(value);

    if (VARATT_IS_EXTERNAL(txt) || VARATT_IS_COMPRESSED(txt))
        txt = PG_DETOAST_DATUM_PACKED(value);

    *text_ptr = txt;
    *len = VARSIZE_ANY_EXHDR(txt);
    *data_ptr = VARDATA_ANY(txt);
}

static Size
codec_measure_int4(AttrCodec *codec, Datum value, bool isnull,
                   struct varlena **text_ptr, const char **text_data, int32 *text_len)
{
    (void) codec;
    (void) value;
    (void) text_ptr;
    (void) text_data;
    (void) text_len;
    return isnull ? 0 : sizeof(int32);
}

static Size
codec_measure_int8(AttrCodec *codec, Datum value, bool isnull,
                   struct varlena **text_ptr, const char **text_data, int32 *text_len)
{
    (void) codec;
    (void) value;
    (void) text_ptr;
    (void) text_data;
    (void) text_len;
    return isnull ? 0 : sizeof(int64);
}

static Size
codec_measure_text(AttrCodec *codec, Datum value, bool isnull,
                   struct varlena **text_ptr, const char **text_data, int32 *text_len)
{
    (void) codec;

    if (isnull)
        return 0;

    prepare_text_datum(value, text_ptr, text_data, text_len);
    return sizeof(int32) + *text_len;
}

static void
codec_encode_int4(AttrCodec *codec, char **dst, Datum value,
                  struct varlena *text_ptr, const char *text_data, int32 text_len)
{
    int32 val = DatumGetInt32(value);

    (void) codec;
    (void) text_ptr;
    (void) text_data;
    (void) text_len;
    memcpy(*dst, &val, sizeof(int32));
    *dst += sizeof(int32);
}

static void
codec_encode_int8(AttrCodec *codec, char **dst, Datum value,
                  struct varlena *text_ptr, const char *text_data, int32 text_len)
{
    int64 val = DatumGetInt64(value);

    (void) codec;
    (void) text_ptr;
    (void) text_data;
    (void) text_len;
    memcpy(*dst, &val, sizeof(int64));
    *dst += sizeof(int64);
}

static void
codec_encode_text(AttrCodec *codec, char **dst, Datum value,
                  struct varlena *text_ptr, const char *text_data, int32 text_len)
{
    (void) codec;
    memcpy(*dst, &text_len, sizeof(int32));
    *dst += sizeof(int32);
    memcpy(*dst, text_data, text_len);
    *dst += text_len;
    if (PointerGetDatum(text_ptr) != value)
        pfree(text_ptr);
}

static Datum
codec_decode_int4(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt)
{
    int32 val;

    (void) codec;
    (void) mcxt;
    *isnull = false;
    memcpy(&val, *src, sizeof(int32));
    *src += sizeof(int32);
    return Int32GetDatum(val);
}

static Datum
codec_decode_int8(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt)
{
    int64 val;

    (void) codec;
    (void) mcxt;
    *isnull = false;
    memcpy(&val, *src, sizeof(int64));
    *src += sizeof(int64);
    return Int64GetDatum(val);
}

static Datum
codec_decode_text(AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt)
{
    int32 len;
    text *txt;
    MemoryContext oldctx;

    (void) codec;
    *isnull = false;
    memcpy(&len, *src, sizeof(int32));
    *src += sizeof(int32);

    oldctx = MemoryContextSwitchTo(mcxt);
    txt = (text *) palloc(VARHDRSZ + len);
    SET_VARSIZE(txt, VARHDRSZ + len);
    memcpy(VARDATA(txt), *src, len);
    MemoryContextSwitchTo(oldctx);

    *src += len;
    return PointerGetDatum(txt);
}

static void
codec_skip_int4(AttrCodec *codec, const char **src)
{
    (void) codec;
    *src += sizeof(int32);
}

static void
codec_skip_int8(AttrCodec *codec, const char **src)
{
    (void) codec;
    *src += sizeof(int64);
}

static void
codec_skip_text(AttrCodec *codec, const char **src)
{
    int32 len;

    (void) codec;
    memcpy(&len, *src, sizeof(int32));
    *src += sizeof(int32) + len;
}

static void
init_row_encoding_cache_hash(void)
{
    HASHCTL info;

    if (row_encoding_cache != NULL)
        return;

    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(RowEncodingCacheEntry);
    info.hcxt = TopMemoryContext;

    row_encoding_cache = hash_create("PostgresRocks row encoding cache",
                                     128,
                                     &info,
                                     HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

RowEncodingCacheEntry *
get_row_encoding_cache(Relation relation)
{
    RowEncodingCacheEntry *entry;
    TupleDesc tupdesc = RelationGetDescr(relation);
    bool found;
    int i;

    init_row_encoding_cache_hash();

    entry = (RowEncodingCacheEntry *) hash_search(row_encoding_cache,
                                                  &relation->rd_id,
                                                  HASH_ENTER,
                                                  &found);

    if (!found || entry->natts != tupdesc->natts || entry->codecs == NULL)
    {
        if (entry->codecs != NULL)
            pfree(entry->codecs);

        entry->table_oid = relation->rd_id;
        entry->natts = tupdesc->natts;
        entry->nullmap_size = NULL_BITMAP_SIZE(entry->natts);
        entry->offset_table_size = sizeof(uint32) * entry->natts;
        entry->base_row_size = sizeof(RowHeader) + entry->nullmap_size + entry->offset_table_size;
        entry->codecs = MemoryContextAllocZero(TopMemoryContext,
                                               sizeof(AttrCodec) * entry->natts);

        for (i = 0; i < entry->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            AttrCodec *codec = &entry->codecs[i];

            codec->type_oid = attr->atttypid;

            switch (attr->atttypid)
            {
                case INT4OID:
                    codec->kind = ATTR_CODEC_INT4;
                    codec->inline_size = sizeof(int32);
                    codec->measure = codec_measure_int4;
                    codec->encode = codec_encode_int4;
                    codec->decode = codec_decode_int4;
                    codec->skip = codec_skip_int4;
                    break;
                case INT8OID:
                    codec->kind = ATTR_CODEC_INT8;
                    codec->inline_size = sizeof(int64);
                    codec->measure = codec_measure_int8;
                    codec->encode = codec_encode_int8;
                    codec->decode = codec_decode_int8;
                    codec->skip = codec_skip_int8;
                    break;
                case TEXTOID:
                case VARCHAROID:
                    codec->kind = ATTR_CODEC_TEXT;
                    codec->inline_size = sizeof(int32);
                    codec->measure = codec_measure_text;
                    codec->encode = codec_encode_text;
                    codec->decode = codec_decode_text;
                    codec->skip = codec_skip_text;
                    break;
                default:
                    elog(ERROR, "Unsupported data type for row encoding cache: %u",
                         attr->atttypid);
            }
        }
    }

    return entry;
}

RowEncodingCacheEntry *
get_row_encoding_cache_by_oid(Oid table_oid)
{
    RowEncodingCacheEntry *entry;
    bool found;
    Relation rel;

    init_row_encoding_cache_hash();

    entry = (RowEncodingCacheEntry *) hash_search(row_encoding_cache,
                                                  &table_oid,
                                                  HASH_FIND,
                                                  &found);
    if (found && entry != NULL && entry->codecs != NULL)
        return entry;

    rel = table_open(table_oid, AccessShareLock);
    entry = get_row_encoding_cache(rel);
    table_close(rel, AccessShareLock);
    return entry;
}

static RowSerializeScratch
get_row_serialize_scratch(int natts)
{
    RowSerializeScratch scratch;

    ensure_serialize_attr_capacity(natts);
    scratch.values = serialize_attr_values;
    scratch.isnulls = serialize_attr_isnull;
    scratch.texts = serialize_attr_texts;
    scratch.text_data = serialize_attr_text_data;
    scratch.text_lens = serialize_attr_text_lens;
    return scratch;
}

static size_t
prepare_row_for_serialization(TupleTableSlot *slot, RowEncodingCacheEntry *cache,
                              RowSerializeScratch *scratch)
{
    size_t total_size = cache->base_row_size;
    int i;

    for (i = 0; i < cache->natts; i++)
    {
        AttrCodec *codec = &cache->codecs[i];

        scratch->values[i] = slot_getattr(slot, i + 1, &scratch->isnulls[i]);
        scratch->texts[i] = NULL;
        scratch->text_data[i] = NULL;
        scratch->text_lens[i] = 0;
        slot->tts_values[i] = scratch->values[i];
        slot->tts_isnull[i] = scratch->isnulls[i];

        total_size += codec->measure(codec,
                                     scratch->values[i],
                                     scratch->isnulls[i],
                                     &scratch->texts[i],
                                     &scratch->text_data[i],
                                     &scratch->text_lens[i]);
    }

    return total_size;
}

static char *
write_row_header_and_nullmap(char *data, RowEncodingCacheEntry *cache,
                             RowSerializeScratch *scratch)
{
    RowHeader header;
    char *ptr = data;
    int i;

    header.natts = cache->natts;
    header.data_len = 0;
    memcpy(ptr, &header, sizeof(RowHeader));
    ptr += sizeof(RowHeader);

    memset(ptr, 0, cache->nullmap_size);
    for (i = 0; i < cache->natts; i++)
    {
        if (scratch->isnulls[i])
            row_set_attr_null((unsigned char *) ptr, i);
    }
    ptr += cache->nullmap_size;

    return ptr;
}

static uint32 *
write_row_offset_table(char **ptr, RowEncodingCacheEntry *cache,
                       RowSerializeScratch *scratch)
{
    uint32 *offsets = (uint32 *) *ptr;
    int i;

    for (i = 0; i < cache->natts; i++)
    {
        if (scratch->isnulls[i])
            offsets[i] = ROW_ATTR_OFFSET_NULL;
        else
            offsets[i] = 0;
    }

    *ptr += cache->offset_table_size;
    return offsets;
}

static void
finalize_row_header(char *data, uint32 natts, uint32 data_len)
{
    RowHeader *header = (RowHeader *) data;

    header->natts = natts;
    header->data_len = data_len;
}

static void
encode_serialized_attributes(char **ptr, uint32 *offsets, RowEncodingCacheEntry *cache,
                             RowSerializeScratch *scratch)
{
    int i;
    char *row_base = *ptr - cache->base_row_size;

    for (i = 0; i < cache->natts; i++)
    {
        AttrCodec *codec = &cache->codecs[i];

        if (!scratch->isnulls[i])
        {
            offsets[i] = (uint32) (*ptr - row_base);
            codec->encode(codec,
                          ptr,
                          scratch->values[i],
                          scratch->texts[i],
                          scratch->text_data[i],
                          scratch->text_lens[i]);
        }
    }
}

char *
serialize_row(TupleTableSlot *slot, RowEncodingCacheEntry *cache, size_t *data_len)
{
    char *data;
    char *ptr;
    uint32 *offsets;
    size_t total_size;
    RowSerializeScratch scratch;
    TimestampTz start_ts = GetCurrentTimestamp();

    scratch = get_row_serialize_scratch(cache->natts);
    total_size = prepare_row_for_serialization(slot, cache, &scratch);

    data = palloc(total_size);
    ptr = write_row_header_and_nullmap(data, cache, &scratch);
    offsets = write_row_offset_table(&ptr, cache, &scratch);
    encode_serialized_attributes(&ptr, offsets, cache, &scratch);
    finalize_row_header(data, (uint32) cache->natts, (uint32) total_size);

    *data_len = total_size;
    postgresrocks_serialize_calls++;
    postgresrocks_serialize_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
    return data;
}

char *
serialize_row_reuse(TupleTableSlot *slot, RowEncodingCacheEntry *cache, size_t *data_len)
{
    char *data;
    char *ptr;
    uint32 *offsets;
    size_t total_size;
    RowSerializeScratch scratch;
    TimestampTz start_ts = GetCurrentTimestamp();

    scratch = get_row_serialize_scratch(cache->natts);
    total_size = prepare_row_for_serialization(slot, cache, &scratch);

    if (serialize_buffer_capacity < total_size)
    {
        size_t new_capacity = Max(total_size,
                                  serialize_buffer_capacity == 0 ? 1024 : serialize_buffer_capacity * 2);

        if (serialize_buffer == NULL)
            serialize_buffer = MemoryContextAlloc(TopMemoryContext, new_capacity);
        else
            serialize_buffer = repalloc(serialize_buffer, new_capacity);

        serialize_buffer_capacity = new_capacity;
    }

    data = serialize_buffer;
    ptr = write_row_header_and_nullmap(data, cache, &scratch);
    offsets = write_row_offset_table(&ptr, cache, &scratch);
    encode_serialized_attributes(&ptr, offsets, cache, &scratch);
    finalize_row_header(data, (uint32) cache->natts, (uint32) total_size);

    *data_len = total_size;
    postgresrocks_serialize_calls++;
    postgresrocks_serialize_us += TimestampDifferenceMicroseconds(start_ts, GetCurrentTimestamp());
    return data;
}

text *
decode_text_attribute_from_row(const char *row_data, RowEncodingCacheEntry *cache, int target_index)
{
    RowHeader *header = (RowHeader *) row_data;
    unsigned char *null_bitmap;
    uint32 *offsets;
    const char *ptr;

    if (header->natts != cache->natts)
        elog(ERROR, "Row attribute count mismatch: expected %d, got %u",
             cache->natts, header->natts);

    null_bitmap = (unsigned char *) (row_data + sizeof(RowHeader));
    offsets = (uint32 *) ((char *) null_bitmap + cache->nullmap_size);

    if (target_index < 0 || target_index >= cache->natts)
        return NULL;
    if (row_attr_is_null(null_bitmap, target_index) ||
        offsets[target_index] == ROW_ATTR_OFFSET_NULL)
        return NULL;
    if (cache->codecs[target_index].kind != ATTR_CODEC_TEXT)
        elog(ERROR, "Requested text decode for non-text codec kind %d",
             (int) cache->codecs[target_index].kind);

    ptr = row_data + offsets[target_index];
    {
        bool decoded_isnull = false;
        Datum value;

        value = cache->codecs[target_index].decode(&cache->codecs[target_index],
                                                   &ptr,
                                                   &decoded_isnull,
                                                   CurrentMemoryContext);
        if (decoded_isnull)
            return NULL;
        return DatumGetTextPP(value);
    }
}
