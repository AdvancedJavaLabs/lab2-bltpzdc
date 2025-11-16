cmake_minimum_required(VERSION 3.10)
project(loader)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} ${LIBPQXX_LIBRARIES})
target_include_directories(${PROJECT_NAME} PRIVATE ${LIBPQXX_INCLUDE_DIRS})
target_compile_options(${PROJECT_NAME} PRIVATE ${LIBPQXX_CFLAGS_OTHER})
