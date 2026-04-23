# SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-FileCopyrightText: 2024–2025 Knode.ai
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(sql_parser_library_dbgen_test LANGUAGES C)

find_library(M_LIB m)

find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(the_io_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(dbgen_test
  src/main.c
)

target_include_directories(dbgen_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(dbgen_test PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET sql_parser_library::sql_parser_library)
  find_package(sql_parser_library CONFIG REQUIRED)
endif()
# target_link_libraries(dbgen_test PRIVATE sql_parser_library::sql_parser_library)

target_link_libraries(dbgen_test PRIVATE
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  the_io_library::the_io_library
  sql_parser_library::sql_parser_library
)

if(M_LIB)
  target_link_libraries(dbgen_test PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(dbgen_test PRIVATE /W4)
else()
  target_compile_options(dbgen_test PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS dbgen_test RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
