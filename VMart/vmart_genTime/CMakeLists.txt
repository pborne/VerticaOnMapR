cmake_minimum_required(VERSION 3.7)
project(vmart_genTime)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        vmart_genTime.c)

add_executable(vmart_genTime ${SOURCE_FILES})