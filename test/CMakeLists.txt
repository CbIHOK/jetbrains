cmake_minimum_required (VERSION 3.8)

add_subdirectory( regression )
add_subdirectory( performance )
add_subdirectory( stress )

add_custom_target( tests ALL )
add_dependencies( tests regression performance stress )