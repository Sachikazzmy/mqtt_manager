#include "gateway_app.h"

#include "payload.h"
#include "simulator.h"
#include "transport.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static simulator_mode_t parse_mode(const char *value) {
    if (strcmp(value, "test") == 0) return SIM_MODE_TEST;
    if (strcmp(value, "random") == 0) return SIM_MODE_RANDOM;
    return SIM_MODE_NORMAL;
}

static void usage(const char *program) {
    fprintf(stderr, "Usage: %s [--mode normal|test|random] [--interval seconds] [--once]\n", program);
}

int app_run(int argc, char **argv) {
    simulator_mode_t mode = SIM_MODE_NORMAL;
    unsigned int interval = 10;
    int once = 0;
    int i;

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--once") == 0) {
            once = 1;
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = parse_mode(argv[++i]);
        } else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) {
            interval = (unsigned int)strtoul(argv[++i], NULL, 10);
            if (interval == 0) interval = 1;
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    simulator_init(mode);

    do {
        sensor_data_t data;
        char payload[2048];
        int length;

        if (simulator_next(&data) != 0) {
            fprintf(stderr, "failed to read CPU temperature\n");
            return 1;
        }

        length = payload_build(payload, sizeof(payload), &data);
        if (length < 0) {
            fprintf(stderr, "failed to build payload\n");
            return 1;
        }

        printf("sequence=%lu %s\n", data.sequence, payload);
        fflush(stdout);

        if (getenv("MQTT_SEND") != NULL &&
            strcmp(getenv("MQTT_SEND"), "1") == 0 &&
            transport_publish(payload, (size_t)length) != 0) {
            fprintf(stderr, "failed to publish payload\n");
            return 1;
        }

        if (!once) sleep(interval);
    } while (!once);

    return 0;
}
