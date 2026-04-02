#ifndef POSTGRESROCKS_CODEC_H
#define POSTGRESROCKS_CODEC_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

typedef struct RowHeader
{
    uint32 natts;
    uint32 data_len;
} RowHeader;

typedef enum AttrCodecKind
{
    ATTR_CODEC_INT4 = 1,
    ATTR_CODEC_INT8 = 2,
    ATTR_CODEC_TEXT = 3
} AttrCodecKind;

typedef struct AttrCodec AttrCodec;

struct AttrCodec
{
    Oid type_oid;
    AttrCodecKind kind;
    uint16 inline_size;
    Size (*measure) (AttrCodec *codec, Datum value, bool isnull,
                     struct varlena **text_ptr, const char **text_data, int32 *text_len);
    void (*encode) (AttrCodec *codec, char **dst, Datum value,
                    struct varlena *text_ptr, const char *text_data, int32 text_len);
    Datum (*decode) (AttrCodec *codec, const char **src, bool *isnull, MemoryContext mcxt);
    void (*skip) (AttrCodec *codec, const char **src);
};

typedef struct RowEncodingCacheEntry
{
    Oid table_oid;
    int natts;
    size_t nullmap_size;
    size_t offset_table_size;
    size_t base_row_size;
    AttrCodec *codecs;
} RowEncodingCacheEntry;

#define NULL_BITMAP_SIZE(natts) (((natts) + 7) / 8)
#define ROW_ATTR_OFFSET_NULL UINT32_MAX

static inline bool
row_attr_is_null(const unsigned char *null_bitmap, int attno)
{
    return (null_bitmap[attno >> 3] & (1U << (attno & 7))) != 0;
}

static inline void
row_set_attr_null(unsigned char *null_bitmap, int attno)
{
    null_bitmap[attno >> 3] |= (1U << (attno & 7));
}

RowEncodingCacheEntry *get_row_encoding_cache(Relation relation);
RowEncodingCacheEntry *get_row_encoding_cache_by_oid(Oid table_oid);
char *serialize_row(TupleTableSlot *slot, RowEncodingCacheEntry *cache, size_t *data_len);
char *serialize_row_reuse(TupleTableSlot *slot, RowEncodingCacheEntry *cache, size_t *data_len);
text *decode_text_attribute_from_row(const char *row_data, RowEncodingCacheEntry *cache, int target_index);
extern uint64 postgresrocks_serialize_calls;
extern uint64 postgresrocks_serialize_us;

#endif
