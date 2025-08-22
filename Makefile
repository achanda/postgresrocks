MODULES = postgresrocks
EXTENSION = postgresrocks
DATA = postgresrocks--0.0.1.sql

# Detect operating system
UNAME_S := $(shell uname -s)

# macOS-specific configuration
ifeq ($(UNAME_S),Darwin)
    # Homebrew paths (both Intel and Apple Silicon)
    HOMEBREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo /usr/local)
    ROCKSDB_HOMEBREW := $(HOMEBREW_PREFIX)/opt/rocksdb
    
    # Check if RocksDB is installed via Homebrew
    ifneq (,$(wildcard $(ROCKSDB_HOMEBREW)))
        PG_CPPFLAGS += -I$(ROCKSDB_HOMEBREW)/include
        PG_LDFLAGS += -L$(ROCKSDB_HOMEBREW)/lib
        LIBS += -lrocksdb -lstdc++
    else
        # Fallback to standard paths
        PG_CPPFLAGS += -I$(HOMEBREW_PREFIX)/include -I/usr/local/include
        PG_LDFLAGS += -L$(HOMEBREW_PREFIX)/lib -L/usr/local/lib
        LIBS += -lrocksdb -lstdc++
    endif
    
    # macOS-specific flags
    PG_CPPFLAGS += -mmacosx-version-min=11.0
    
else
    # Linux/other Unix systems
    LIBS += -lrocksdb -lstdc++
    PG_CPPFLAGS += -I/usr/local/include
    PG_LDFLAGS += -L/usr/local/lib
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Custom rule to link with RocksDB properly
%.dylib: %.o
ifeq ($(UNAME_S),Darwin)
	$(CC) $(LDFLAGS_SL) $(LDFLAGS) $(CFLAGS) $< -L/opt/homebrew/opt/rocksdb/lib -lrocksdb -lstdc++ $(BE_DLLLIBS) -bundle -bundle_loader $(bindir)/postgres -o $@
else
	$(CC) $(LDFLAGS_SL) $(LDFLAGS) $< -lrocksdb -lstdc++ $(BE_DLLLIBS) -o $@
endif
