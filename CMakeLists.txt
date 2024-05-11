cmake_minimum_required(VERSION 3.10)
project(qdrant_fdw C)

set(CMAKE_C_STANDARD 17)

execute_process(COMMAND pg_config --includedir-server OUTPUT_VARIABLE PG_INCLUDE OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND pg_config --sharedir OUTPUT_VARIABLE PG_SHARE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND pg_config --libdir OUTPUT_VARIABLE PG_LIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)

find_package(CURL REQUIRED)

add_subdirectory(cJSON)

add_library(qdrant_fdw SHARED 
        qdrant_fdw.c
        filter.c)
target_include_directories(qdrant_fdw PRIVATE ${CURL_INCLUDE_DIR} ${PG_INCLUDE})
target_link_libraries(qdrant_fdw PRIVATE curl cjson)

SET_TARGET_PROPERTIES(qdrant_fdw PROPERTIES PREFIX "")
install(TARGETS qdrant_fdw DESTINATION ${PG_LIB_DIR})
install(FILES qdrant_fdw.control qdrant_fdw--1.0.sql DESTINATION ${PG_SHARE_DIR}/extension)
