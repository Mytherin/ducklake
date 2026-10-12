add_library(
  ducklake_rest OBJECT
  ducklake_rest_catalog_changes.cpp ducklake_rest_client.cpp
  ducklake_rest_endpoints.cpp ducklake_rest_json.cpp
  ducklake_rest_serialization.cpp ducklake_rest_server.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_rest>
    PARENT_SCOPE)
