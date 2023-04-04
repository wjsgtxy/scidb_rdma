set (json_src
    json_reader.cpp
    json_writer.cpp
    json_value.cpp
)

# Disabling -Wconversion for the JSON library makes it easier to drop
# in a new version later.
set_source_files_properties(${json_src}
  PROPERTIES
  COMPILE_FLAGS -Wno-conversion
  )

add_library(json_lib STATIC ${json_src})
