cmake_minimum_required(VERSION 3.23)
project(cpp)

set(CMAKE_CXX_STANDARD 14)


find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})

include_directories(jni)

add_subdirectory(src)
