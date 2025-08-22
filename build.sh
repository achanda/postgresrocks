#!/bin/bash

# Build script for PostgresRocks extension

set -e

echo "Building PostgresRocks extension..."

# Detect operating system
OS="$(uname -s)"

# macOS-specific checks and setup
if [ "$OS" = "Darwin" ]; then
    echo "Detected macOS - checking dependencies..."
    
    # Check if Homebrew is installed
    if ! command -v brew &> /dev/null; then
        echo "Warning: Homebrew not found. Install with:"
        echo "/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
    fi
    
    # Check if RocksDB is installed
    if ! brew list rocksdb &> /dev/null; then
        echo "RocksDB not found. Installing via Homebrew..."
        brew install rocksdb
    else
        echo "RocksDB found: $(brew --prefix rocksdb)"
    fi
    
    # Check if PostgreSQL development headers are available
    if ! command -v pg_config &> /dev/null; then
        echo "PostgreSQL development tools not found. Installing..."
        brew install postgresql
    else
        echo "PostgreSQL found: $(pg_config --version)"
        
        # Check if PGXS is available (needed for extensions)
        PGXS_PATH="$(pg_config --pgxs)"
        if [ ! -f "$PGXS_PATH" ]; then
            echo "PGXS not found at: $PGXS_PATH"
            echo "Installing full PostgreSQL package..."
            
            # Check if libpq is linked and causing conflicts
            if brew list --versions libpq > /dev/null 2>&1; then
                echo "Found conflicting libpq installation. Unlinking it..."
                brew unlink libpq
            fi
            
            # Install PostgreSQL (latest version for best compatibility)
            brew install postgresql
            
            # Link the full PostgreSQL package
            echo "Linking PostgreSQL..."
            brew link postgresql
            
            # Re-check after installation
            if [ ! -f "$(pg_config --pgxs)" ]; then
                echo "Error: PGXS still not found after PostgreSQL installation."
                echo "Available PostgreSQL versions:"
                brew list | grep postgresql
                exit 1
            fi
        fi
    fi
    
    # Set macOS-specific environment
    export MACOSX_DEPLOYMENT_TARGET=10.15
fi

# Clean previous builds
make clean

# Build the extension
make

echo "Build completed successfully!"

# Check if we should install
if [ "$1" = "install" ]; then
    echo "Installing extension..."
    if [ "$OS" = "Darwin" ]; then
        # On macOS, might not need sudo depending on PostgreSQL installation
        make install || sudo make install
    else
        sudo make install
    fi
    echo "Installation completed!"
fi

# Check if we should test
if [ "$1" = "test" ] || [ "$2" = "test" ]; then
    echo "Running tests..."
    echo "Make sure PostgreSQL is running and you have created a test database."
    echo "Run: psql -d <your_database> -f test.sql"
    
    if [ "$OS" = "Darwin" ]; then
        echo ""
        echo "macOS-specific notes:"
        echo "- Start PostgreSQL: brew services start postgresql"
        echo "- Create database: createdb testdb"
        echo "- Run tests: psql -d testdb -f test.sql"
    fi
fi

echo "Done!"
