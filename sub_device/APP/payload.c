#include "payload.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int build_sampled_at(char *buffer, size_t buffer_size) {
    time_t now = time(NULL);
    if (now == (time_t)-1) {
        return -1;
    }

    struct tm *utc_time = gmtime(&now);
    if (utc_time == NULL) {
        return -1;
    }

    int written = snprintf(
        buffer,
        buffer_size,
        "%04d-%02d-%02dT%02d:%02d:%02d.000Z",
        utc_time->tm_year + 1900,
        utc_time->tm_mon + 1,
        utc_time->tm_mday,
        utc_time->tm_hour,
        utc_time->tm_min,
        utc_time->tm_sec
    );

    if (written < 0 || (size_t)written >= buffer_size) {
        return -1;
    }

    return 0;
}

static int build_message_id(char *buffer, size_t buffer_size) {
    time_t now = time(NULL);
    if (now == (time_t)-1) {
        return -1;
    }

    unsigned int random_part = (unsigned int)rand();
    unsigned long long clock_part = (unsigned long long)clock();
    unsigned long long address_part = (unsigned long long)(uintptr_t)buffer;

    int written = snprintf(
        buffer,
        buffer_size,
        "device-001-%lld-%llu-%llu-%u",
        (long long)now,
        clock_part,
        address_part,
        random_part
    );

    if (written < 0 || (size_t)written >= buffer_size) {
        return -1;
    }

    return 0;
}

int payload_build(char *buffer, size_t buffer_size) {
    char sampled_at[32];
    char message_id[128];

    srand((unsigned int)time(NULL) ^ (unsigned int)clock() ^ (unsigned int)(uintptr_t)buffer);

    if (build_sampled_at(sampled_at, sizeof(sampled_at)) != 0) {
        return -1;
    }

    if (build_message_id(message_id, sizeof(message_id)) != 0) {
        return -1;
    }

    int written = snprintf(
        buffer,
        buffer_size,
        "{"
        "\"version\":\"1\","
        "\"device_id\":\"device-001\","
        "\"message_id\":\"%s\","
        "\"sampled_at\":\"%s\","
        "\"metrics\":{"
        "\"temperature\":{\"value\":23.6,\"unit\":\"C\"},"
        "\"pressure\":{\"value\":101.3,\"unit\":\"kPa\"},"
        "\"current\":{\"value\":2.5,\"unit\":\"A\"}"
        "}"
        "}",
        message_id,
        sampled_at
    );

    if (written < 0 || (size_t)written >= buffer_size) {
        return -1;
    }

    return written;
}
