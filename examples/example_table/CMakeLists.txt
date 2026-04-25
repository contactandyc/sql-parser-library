# SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(sql_parser_library_example_table LANGUAGES C)

find_library(M_LIB m)

find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(example_table
  src/main.c
)

target_include_directories(example_table PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(example_table PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET sql_parser_library::sql_parser_library)
  find_package(sql_parser_library CONFIG REQUIRED)
endif()
# target_link_libraries(example_table PRIVATE sql_parser_library::sql_parser_library)

target_link_libraries(example_table PRIVATE
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  sql_parser_library::sql_parser_library
)

if(M_LIB)
  target_link_libraries(example_table PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(example_table PRIVATE /W4)
else()
  target_compile_options(example_table PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS example_table RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
