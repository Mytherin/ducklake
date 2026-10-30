add_library(
  ducklake_iceberg OBJECT
  iceberg_avro.cpp
  iceberg_avro_functions.cpp
  iceberg_endpoints.cpp
  iceberg_inlined.cpp
  iceberg_properties.cpp
  iceberg_server.cpp
  iceberg_table_export.cpp
  iceberg_table_update.cpp
  iceberg_types.cpp
  iceberg_util.cpp
  iceberg_views.cpp
  iceberg_virtual_files.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_iceberg>
    PARENT_SCOPE)
