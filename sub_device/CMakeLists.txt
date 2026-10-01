cmake_minimum_required(VERSION 3.16)

project(sub_device C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(sub_device
    main.c
    APP/gateway_app.c
    APP/payload.c
    APP/transport.c
)

target_include_directories(sub_device PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    C:/msys64/ucrt64/include
)

target_link_directories(sub_device PRIVATE
    C:/msys64/ucrt64/lib
)

target_link_libraries(sub_device PRIVATE
    paho-mqtt3cs
)


