set(
    EXAMPLE_SOURCES
    pax_example.cpp
)

if(${WITH_V8})
    list(APPEND UNITTEST_SOURCES backend/WasmTestInterpreter.cpp backend/WasmTestV8.cpp)
endif()

include_directories(.)
add_executable(paxtest ${EXAMPLE_SOURCES})
target_link_libraries(paxtest PRIVATE ${PROJECT_NAME}_complete Threads::Threads)