#include "payload.h"

#include <math.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int append_text(char *buffer, size_t buffer_size, size_t *used,
                       const char *format, ...) {
    va_list args;
    int written;

    va_start(args, format);
    written = vsnprintf(buffer + *used, buffer_size - *used, format, args);
    va_end(args);
    if (written < 0 || (size_t)written >= buffer_size - *used) return -1;
    *used += (size_t)written;
    return 0;
}

static int build_sampled_at(char *buffer, size_t buffer_size) {
    time_t now = time(NULL);
    struct tm *utc_time;
    int written;

    if (now == (time_t)-1) return -1;
    utc_time = gmtime(&now);
    if (utc_time == NULL) return -1;
    written = snprintf(buffer, buffer_size,
                       "%04d-%02d-%02dT%02d:%02d:%02dZ",
                       utc_time->tm_year + 1900, utc_time->tm_mon + 1,
                       utc_time->tm_mday, utc_time->tm_hour,
                       utc_time->tm_min, utc_time->tm_sec);
    return written >= 0 && (size_t)written < buffer_size ? 0 : -1;
}

static int next_persistent_sequence(uint64_t *sequence) {
    const char *state_file = getenv("MESSAGE_ID_STATE_FILE");
    char temp_file[512];
    FILE *file;
    unsigned long long value = 0;

    if (state_file == NULL || state_file[0] == '\0') state_file = ".message-sequence";
    file = fopen(state_file, "r");
    if (file != NULL) {
        if (fscanf(file, "%llu", &value) != 1) value = 0;
        fclose(file);
    }
    if (value == UINT64_MAX) return -1;
    value++;
    if (snprintf(temp_file, sizeof(temp_file), "%s.tmp", state_file) >=
        (int)sizeof(temp_file)) return -1;
    file = fopen(temp_file, "w");
    if (file == NULL) return -1;
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
    unsigned char bytes[16];
    FILE *random_file = fopen("/dev/urandom", "rb");
    int written;

    if (random_file != NULL && fread(bytes, 1, sizeof(bytes), random_file) == sizeof(bytes)) {
        fclose(random_file);
        bytes[6] = (unsigned char)((bytes[6] & 0x0f) | 0x40);
        bytes[8] = (unsigned char)((bytes[8] & 0x3f) | 0x80);
        written = snprintf(buffer, buffer_size,
            "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]);
        return written >= 0 && (size_t)written < buffer_size ? 0 : -1;
    }
    if (random_file != NULL) fclose(random_file);

    {
        uint64_t sequence;
        time_t now = time(NULL);
        if (now == (time_t)-1 || next_persistent_sequence(&sequence) != 0) return -1;
        written = snprintf(buffer, buffer_size, "linux-01-%lld-%llu",
                           (long long)now, (unsigned long long)sequence);
    }
    return written >= 0 && (size_t)written < buffer_size ? 0 : -1;
}

int payload_build(char *buffer, size_t buffer_size,
                  const sensor_data_t *data, unsigned int metric_mask) {
    char sampled_at[32];
    char message_id[128];
    size_t used = 0;
    int first_metric = 1;

    if (buffer == NULL || data == NULL || metric_mask == 0 ||
        !isfinite(data->temperature) || !isfinite(data->pressure) ||
        !isfinite(data->current) ||
        build_sampled_at(sampled_at, sizeof(sampled_at)) != 0 ||
        build_message_id(message_id, sizeof(message_id)) != 0) return -1;

    if (append_text(buffer, buffer_size, &used,
                    "{\"version\":\"1\",\"device_id\":\"linux-01\","
                    "\"message_id\":\"%s\",\"sampled_at\":\"%s\","
                    "\"metrics\":{", message_id, sampled_at) != 0) return -1;

#define ADD_METRIC(bit, key, value, unit, modifiable) \
    do { \
        if ((metric_mask & (bit)) != 0) { \
            if (!first_metric && append_text(buffer, buffer_size, &used, ",") != 0) return -1; \
            if (append_text(buffer, buffer_size, &used, \
                "\"%s\":{\"value\":%.2f,\"unit\":\"%s\",\"modifiable\":%s}", \
                (key), (value), (unit), (modifiable)) != 0) return -1; \
            first_metric = 0; \
        } \
    } while (0)

    ADD_METRIC(METRIC_TEMPERATURE, "segment-1", data->temperature, "V", "false");
    ADD_METRIC(METRIC_PRESSURE, "segment-2", data->pressure, "kPa", "true");
    ADD_METRIC(METRIC_CURRENT, "segment-3", data->current, "A", "true");
#undef ADD_METRIC

    if (first_metric || append_text(buffer, buffer_size, &used, "}}") != 0) return -1;
    return (int)used;
}
