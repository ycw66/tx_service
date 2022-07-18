
cmake_minimum_required( VERSION 3.0 )

project( CatchExamples CXX )

find_package(Catch2 REQUIRED)

# define folders used:
set( TESTS_DIR ${CMAKE_SOURCE_DIR}/tests )
set( HEADER_DIR   ${CMAKE_SOURCE_DIR}/tests/include )

# single-file sources:

set( SOURCES_SINGLE_FILE
    CcEntry-Test.cpp
    StartTsCollector-Test.cpp
)

set( SOURCES_ALL
    ${SOURCES_SINGLE_FILE}
)

foreach( name ${SOURCES_ALL} )
    list( APPEND SOURCES_ALL_PATH ${TESTS_DIR}/${name} )
endforeach()

string( REPLACE ".cpp" "" BASENAMES_SINGLE_FILE  "${SOURCES_SINGLE_FILE}" )

set( TARGETS_SINGLE_FILE   ${BASENAMES_SINGLE_FILE} )

set( TARGETS_ALL
    ${TARGETS_SINGLE_FILE}
)

enable_testing()
include(CTest)
include(Catch)

foreach( name ${TARGETS_SINGLE_FILE} )
    add_executable( ${name} ${TESTS_DIR}/${name}.cpp)
    target_link_libraries(${name} PUBLIC txservice Catch2::Catch2)
    catch_discover_tests(${name})
endforeach()

foreach( name ${TARGETS_ALL} )
    target_include_directories( ${name} PRIVATE ${HEADER_DIR} ${CMAKE_SOURCE_DIR} ${INCLUDE_DIR} )

    set_property(TARGET ${name} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${name} PROPERTY CXX_EXTENSIONS OFF)

    # Add desired warnings
    if ( CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang|GNU" )
        target_compile_options( ${name}  PRIVATE -Wall -Wextra -Wunreachable-code )
    endif()
    # Clang specific warning go here
    if ( CMAKE_CXX_COMPILER_ID MATCHES "Clang" )
        # Actually keep these
        target_compile_options( ${name}  PRIVATE -Wweak-vtables -Wexit-time-destructors -Wglobal-constructors -Wmissing-noreturn )
    endif()
    if ( CMAKE_CXX_COMPILER_ID MATCHES "MSVC" )
        target_compile_options( ${name}  PRIVATE /W4 /w44265 /WX )
    endif()
endforeach()

