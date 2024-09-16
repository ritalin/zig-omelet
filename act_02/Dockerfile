
# Build stage (duckdb)
FROM internal-dev-stage AS build_duckdb

WORKDIR /home/linuxbrew
    
# download duckdb source repository
RUN git clone --depth=1 https://github.com/duckdb/duckdb.git

# apply monly patch from issue 12657 (https://github.com/duckdb/duckdb/issues/12657)
# RUN target_file="duckdb/extension/jemalloc/jemalloc/include/jemalloc/internal/jemalloc_internal_defs.h" \
#     && line_number=485 \
#     && sed -i "${line_number}s/^\/\/ #define JEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE/#define JEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE/" "$target_file"

# add homebre prefix to env
ENV BREW_PREFIX="/home/linuxbrew/.linuxbrew/opt"
# configure clang
ENV LD_LIBRARY_PATH="$BREW_PREFIX/llvm/lib:$LD_LIBRARY_PATH"

# build duckdb

# RUN ls -l $BREW_PREFIX/llvm/lib/libc++.so*

RUN rm -rf duckdb/_build
RUN mkdir -p duckdb/_build \
    && cmake -GNinja \
        -DCMAKE_C_COMPILER=$BREW_PREFIX/llvm/bin/clang \
        -DCMAKE_CXX_COMPILER=$BREW_PREFIX/llvm/bin/clang++ \
        -DCMAKE_VERBOSE_MAKEFILE=ON \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++" \
        -DCMAKE_EXE_LINKER_FLAGS="-stdlib=libc++ -L$BREW_PREFIX/llvm/lib -Wl,-rpath,$BREW_PREFIX/llvm/lib" \
        -DCMAKE_PREFIX_PATH="$BREW_PREFIX/llvm" \
        -DDUCKDB_EXPLICIT_PLATFORM="linux_amd64" \
        -DCMAKE_BUILD_TYPE=Release \
        -DINSTALL_LIB_DIR=duckdb/_install/lib \
        -DINSTALL_INCLUDE_DIR=duckdb/_install/include \
        -DINSTALL_BIN_DIR=duckdb/_install/bin \
        -DINSTALL_CMAKE_DIR=duckdb/_install/lib/cmake/DuckDB \
        -DBUILD_EXTENSIONS='autocomplete;icu;parquet;json' \
        -DENABLE_EXTENSION_AUTOLOADING=1 \
        -DENABLE_EXTENSION_AUTOINSTALL=1 \
        -S duckdb \
        -B duckdb/_build

RUN cmake --build duckdb/_build
