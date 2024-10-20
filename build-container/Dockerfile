# -------------------
# Build tool setup
# -------------------
FROM homebrew/brew AS dev

# Update and install common packages
RUN brew update 
RUN brew install llvm && \
    brew install cmake && \
    brew install ninja

# -------------------
# Build stage (duckdb)
# -------------------
FROM dev AS build_duckdb

WORKDIR /home/linuxbrew
    
# download duckdb source repository
RUN git clone --depth=1 --branch main https://github.com/duckdb/duckdb.git

# add homebre prefix to env
ENV BREW_PREFIX="/home/linuxbrew/.linuxbrew/opt"
# configure clang
ENV LD_LIBRARY_PATH="$BREW_PREFIX/llvm/lib:$LD_LIBRARY_PATH"

# build duckdb

RUN mkdir -p duckdb/_build \
    && cmake -GNinja \
        -DCMAKE_C_COMPILER=$BREW_PREFIX/llvm/bin/clang \
        -DCMAKE_CXX_COMPILER=$BREW_PREFIX/llvm/bin/clang++ \
        -DCMAKE_VERBOSE_MAKEFILE=ON \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++ -I$BREW_PREFIX/llvm/include/c++/v1" \
        -DCMAKE_SHARED_LINKER_FLAGS="-L/home/linuxbrew/.linuxbrew/opt/llvm/lib -Wl,-Bstatic -lc++ -lc++abi -lunwind -Wl,-Bdynamic" \
        -DCMAKE_EXE_LINKER_FLAGS="-L/home/linuxbrew/.linuxbrew/opt/llvm/lib -Wl,-Bstatic -lc++ -lc++abi -lunwind -Wl,-Bdynamic" \
        -DDUCKDB_EXPLICIT_PLATFORM="linux_amd64" \
        -DCMAKE_BUILD_TYPE=Release \
        -DINSTALL_LIB_DIR=duckdb/_install/lib \
        -DINSTALL_INCLUDE_DIR=duckdb/_install/include \
        -DINSTALL_BIN_DIR=duckdb/_install/bin \
        -DINSTALL_CMAKE_DIR=duckdb/_install/lib/cmake/DuckDB \
        -DBUILD_EXTENSIONS='autocomplete;icu;parquet;json' \
        -DENABLE_EXTENSION_AUTOLOADING=1 \
        -DENABLE_EXTENSION_AUTOINSTALL=1 \
        -DSTATIC_LIBCPP=1 \
        -S duckdb \
        -B duckdb/_build

RUN cmake --build duckdb/_build

# -------------------
# install stage (duckdb)
# -------------------
FROM build_duckdb AS install_duckdb

WORKDIR /home/linuxbrew

RUN cmake --install duckdb/_build

# -------------------
# Build stage (catch2)
# -------------------
FROM dev AS build_catch2

WORKDIR /home/linuxbrew

RUN git clone --depth=1 --branch devel https://github.com/catchorg/Catch2.git catch2

# add homebre prefix to env
ENV BREW_PREFIX="/home/linuxbrew/.linuxbrew/opt"
# configure clang
ENV LD_LIBRARY_PATH="$BREW_PREFIX/llvm/lib:$LD_LIBRARY_PATH"

RUN mkdir -p catch2/_build \
&& cmake -GNinja \
    -DCMAKE_C_COMPILER=$BREW_PREFIX/llvm/bin/clang \
    -DCMAKE_CXX_COMPILER=$BREW_PREFIX/llvm/bin/clang++ \
    -DCMAKE_CXX_FLAGS="-stdlib=libc++ -I$BREW_PREFIX/llvm/include/c++/v1" \
    -DCMAKE_EXE_LINKER_FLAGS="-stdlib=libc++ -L$BREW_PREFIX/llvm/lib -lc++abi" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=catch2/_install \
    -S catch2 \
    -B catch2/_build

RUN cmake --build catch2/_build

# -------------------
# install stage (catch2)
# -------------------
FROM build_catch2 AS install_catch2

WORKDIR /home/linuxbrew

RUN cmake --install catch2/_build

# -------------------
# install stage (other packages)
# -------------------
FROM dev AS install_pkg

WORKDIR /home/linuxbrew

RUN brew install zeromq && \
    brew install rsync && \
    brew install git

# resolve symlinks
RUN rsync -aL ~/.linuxbrew/opt/zeromq /tmp/ && \
    rsync -a --delete /tmp/zeromq ~/.linuxbrew/opt/

RUN rsync -aL ~/.linuxbrew/opt/libsodium /tmp/ && \
    rsync -a --delete /tmp/libsodium ~/.linuxbrew/opt/

# -------------------
# Last stage
# -------------------
FROM node:slim

WORKDIR /home/node

# copy artifact
COPY --from=install_duckdb /home/linuxbrew/duckdb/_install/. /home/node/duckdb
COPY --from=install_catch2 /home/linuxbrew/catch2/_install/. /home/node/catch2
COPY --from=install_pkg /home/linuxbrew/.linuxbrew/opt/zeromq/. /home/node/zeromq
COPY --from=install_pkg /home/linuxbrew/.linuxbrew/opt/libsodium/. /home/node/libsodium
