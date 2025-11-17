cmake_minimum_required(VERSION 3.10)
project(TextProcessingSystem)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)
pkg_check_modules(RABBITMQ REQUIRED librabbitmq)

set(COMMON_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common)

add_executable(loader loader/main.cpp)
target_link_libraries(loader ${LIBPQXX_LIBRARIES})
target_include_directories(loader PRIVATE 
    ${LIBPQXX_INCLUDE_DIRS}
    ${COMMON_INCLUDE_DIR}
)
target_compile_options(loader PRIVATE ${LIBPQXX_CFLAGS_OTHER})

add_executable(splitter splitter/main.cpp)
target_link_libraries(splitter 
    ${LIBPQXX_LIBRARIES}
    ${RABBITMQ_LIBRARIES}
)
target_include_directories(splitter PRIVATE 
    ${LIBPQXX_INCLUDE_DIRS}
    ${RABBITMQ_INCLUDE_DIRS}
    ${COMMON_INCLUDE_DIR}
)
target_compile_options(splitter PRIVATE 
    ${LIBPQXX_CFLAGS_OTHER}
    ${RABBITMQ_CFLAGS_OTHER}
)

add_executable(worker worker/main.cpp)
target_link_libraries(worker 
    ${RABBITMQ_LIBRARIES}
    ${LIBPQXX_LIBRARIES}
)
target_include_directories(worker PRIVATE 
    ${RABBITMQ_INCLUDE_DIRS}
    ${LIBPQXX_INCLUDE_DIRS}
    ${COMMON_INCLUDE_DIR}
)
target_compile_options(worker PRIVATE 
    ${RABBITMQ_CFLAGS_OTHER}
    ${LIBPQXX_CFLAGS_OTHER}
)

set_target_properties(loader splitter worker PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

