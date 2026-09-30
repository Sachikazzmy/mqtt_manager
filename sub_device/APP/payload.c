#include "payload.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int build_sampled_at(char *buffer, size_t buffer_size) {
    time_t now = time(NULL);
    struct tm *utc_time;
    int written;

    if (now == (time_t)-1) {
        return -1;
    }

    utc_time = gmtime(&now);
    if (utc_time == NULL) {
        return -1;
    }

    written = snprintf(buffer, buffer_size,
                       "%04d-%02d-%02dT%02d:%02d:%02d.000Z",
                       utc_time->tm_year + 1900,
                       utc_time->tm_mon + 1,
                       utc_time->tm_mday,
                       utc_time->tm_hour,
                       utc_time->tm_min,
                       utc_time->tm_sec);

    return written >= 0 && (size_t)written < buffer_size ? 0 : -1;
}

static int next_persistent_sequence(uint64_t *sequence) {
    const char *state_file = getenv("MESSAGE_ID_STATE_FILE");
    char temp_file[512];
    FILE *file;
    unsigned long long value = 0;

    if (state_file == NULL || state_file[0] == '\0') {
        state_file = ".message-sequence";
    }

    file = fopen(state_file, "r");
    if (file != NULL) {
        if (fscanf(file, "%llu", &value) != 1) {
            value = 0;
        }
        fclose(file);
    }

    if (value == UINT64_MAX) {
        return -1;
    }
    value++;

    if (snprintf(temp_file, sizeof(temp_file), "%s.tmp", state_file) >=
        (int)sizeof(temp_file)) {
        return -1;
    }

    file = fopen(temp_file, "w");
    if (file == NULL) {
        return -1;
    }

    if (fprintf(file, "%llu\n", value) < 0 || fclose(file) != 0) {
        remove(temp_file);
        return -1;
    }

    if (rename(temp_file, state_file) != 0) {
        remove(temp_file);
        return -1;
    }

    *sequence = (uint64_t)value;
    return 0;
}

static int build_message_id(char *buffer, size_t buffer_size) {
    uint64_t sequence;
    time_t now = time(NULL);
    int written;

    if (now == (time_t)-1 || next_persistent_sequence(&sequence) != 0) {
        return -1;
    }

    written = snprintf(buffer, buffer_size, "device-001-%lld-%llu",
                       (long long)now,
                       (unsigned long long)sequence);

    return written >= 0 && (size_t)written < buffer_size ? 0 : -1;
}

int payload_build(char *buffer, size_t buffer_size, const sensor_data_t *data) {
    char sampled_at[32];
    char message_id[128];
    int written;

    if (data == NULL || build_sampled_at(sampled_at, sizeof(sampled_at)) != 0 ||
        build_message_id(message_id, sizeof(message_id)) != 0) {
        return -1;
    }

    written = snprintf(
        buffer,
        buffer_size,
        "{"
        "\"version\":\"1\","
        "\"device_id\":\"device-001\","
        "\"message_id\":\"%s\","
        "\"sampled_at\":\"%s\","
        "\"metrics\":{" 
        "\"temperature\":{\"value\":%.2f,\"unit\":\"C\"},"
        "\"pressure\":{\"value\":%.2f,\"unit\":\"kPa\"},"
        "\"current\":{\"value\":%.2f,\"unit\":\"A\"}"
        "}}",
        message_id,
        sampled_at,
        data->temperature,
        data->pressure,
        data->current);

    return written >= 0 && (size_t)written < buffer_size ? written : -1;
}
