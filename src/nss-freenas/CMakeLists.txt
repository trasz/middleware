cmake_minimum_required(VERSION 2.8)

project (nss_freenas)
set(CMAKE_BUILD_TYPE Debug)

find_package (Threads)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(nss_freenas SHARED src/nss_freenas.c)
include_directories(/usr/local/include)
set_target_properties(nss_freenas PROPERTIES PREFIX "")
set_target_properties(nss_freenas PROPERTIES SOVERSION 1)
target_link_libraries(nss_freenas /usr/local/lib/libdispatcherclient.so)
target_link_libraries(nss_freenas ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS nss_freenas LIBRARY DESTINATION lib)
