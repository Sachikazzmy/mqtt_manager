cmake_minimum_required(VERSION 3.16)
project(sub_device C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

option(SUB_DEVICE_ENABLE_MQTT "Build MQTT over WSS publishing support" OFF)

set(TRANSPORT_SOURCE APP/transport_stub.c)
if(SUB_DEVICE_ENABLE_MQTT)
    find_path(PAHO_INCLUDE_DIR MQTTClient.h REQUIRED)
    find_library(PAHO_LIBRARY NAMES paho-mqtt3cs REQUIRED)
    set(TRANSPORT_SOURCE APP/transport.c)
endif()

add_executable(sub_device
    main.c
    APP/gateway_app.c
    APP/payload.c
    APP/simulator.c
    ${TRANSPORT_SOURCE}
)

target_include_directories(sub_device PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

if(SUB_DEVICE_ENABLE_MQTT)
    target_include_directories(sub_device PRIVATE ${PAHO_INCLUDE_DIR})
    target_link_libraries(sub_device PRIVATE ${PAHO_LIBRARY})
else()
    message(STATUS "MQTT disabled: building local simulator only")
endif()
