#include "transport.h"

#include <stdio.h>

int transport_publish(const char *payload, size_t payload_length) {
    (void)payload;
    (void)payload_length;
    fprintf(stderr, "MQTT support is disabled in this build\n");
    return -1;
}
