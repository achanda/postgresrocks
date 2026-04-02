MODULE_big = postgresrocks
OBJS = postgresrocks.o postgresrocks_codec.o
EXTENSION = postgresrocks
DATA = postgresrocks--0.0.1.sql
PG_CONFIG ?= pg_config-18
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)
PG_CTL := $(PG_BINDIR)/pg_ctl

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
        SHLIB_LINK += -lrocksdb -lstdc++
    else
        # Fallback to standard paths
        PG_CPPFLAGS += -I$(HOMEBREW_PREFIX)/include -I/usr/local/include
        PG_LDFLAGS += -L$(HOMEBREW_PREFIX)/lib -L/usr/local/lib
        SHLIB_LINK += -lrocksdb -lstdc++
    endif
    
    # macOS-specific flags
    PG_CPPFLAGS += -mmacosx-version-min=11.0
    
else
    # Linux/other Unix systems
    SHLIB_LINK += -lrocksdb -lstdc++
    PG_CPPFLAGS += -I/usr/local/include
    PG_LDFLAGS += -L/usr/local/lib
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: restart-postgres install-restart paper clean-paper

restart-postgres:
ifeq ($(UNAME_S),Darwin)
	@if command -v brew >/dev/null 2>&1; then \
		brew services restart postgresql@18; \
	elif [ -n "$$PGDATA" ]; then \
		"$(PG_CTL)" -D "$$PGDATA" restart; \
	else \
		echo "Unable to restart PostgreSQL automatically. Install Homebrew PostgreSQL 18 or set PGDATA."; \
		exit 1; \
	fi
else
	@if [ -n "$$PGDATA" ]; then \
		"$(PG_CTL)" -D "$$PGDATA" restart; \
	else \
		echo "Set PGDATA so $(PG_CTL) can restart PostgreSQL."; \
		exit 1; \
	fi
endif

install-restart: all install restart-postgres

paper:
	@if ! command -v latexmk >/dev/null 2>&1; then \
		echo "latexmk is not installed. Install MacTeX or a TeX Live setup with latexmk and acmart."; \
		exit 1; \
	fi
	@kpsewhich acmart.cls >/dev/null 2>&1 || { \
		echo "acmart.cls not found. Install the ACM LaTeX class (usually included with MacTeX/TeX Live)."; \
		exit 1; \
	}
	latexmk -pdf paper.tex

clean-paper:
	@if command -v latexmk >/dev/null 2>&1; then \
		latexmk -C paper.tex; \
	else \
		rm -f paper.pdf paper.aux paper.bbl paper.blg paper.fdb_latexmk paper.fls paper.log paper.out paper.toc; \
	fi
