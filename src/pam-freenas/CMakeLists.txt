cmake_minimum_required(VERSION 2.8)

project (pam_freenas)
set(CMAKE_BUILD_TYPE Debug)

find_package (Threads)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(pam_freenas SHARED src/pam_freenas.c)
include_directories(/usr/local/include)
set_target_properties(pam_freenas PROPERTIES PREFIX "")
set_target_properties(pam_freenas PROPERTIES SOVERSION 1)
target_link_libraries(pam_freenas /usr/local/lib/libdispatcherclient.so pam)
target_link_libraries(pam_freenas ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS pam_freenas LIBRARY DESTINATION lib)
