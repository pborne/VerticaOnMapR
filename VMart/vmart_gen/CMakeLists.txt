cmake_minimum_required(VERSION 3.7)
project(vmart_gen)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        vmart_gen.c)

add_executable(vmart_gen ${SOURCE_FILES})