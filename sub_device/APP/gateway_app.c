#include "gateway_app.h"

#include "interaction.h"
#include "payload.h"
#include "simulator.h"
#include "transport.h"
#include "trigger.h"

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
    fprintf(stderr,
            "Usage: %s [--mode normal|test|random] "
            "[--interval seconds] [--once] [--trigger-test samples]\n",
            program);
}

static void print_reason_names(unsigned int reasons) {
    int first = 1;
    printf("trigger=");
    if (reasons & TRIGGER_PERIODIC) {
        printf("%speriodic", first ? "" : "+");
        first = 0;
    }
    if (reasons & TRIGGER_CHANGE) {
        printf("%schange", first ? "" : "+");
        first = 0;
    }
    if (reasons & TRIGGER_RANGE) {
        printf("%srange", first ? "" : "+");
        first = 0;
    }
    if (reasons & TRIGGER_INTERRUPT) {
        printf("%sinterrupt", first ? "" : "+");
    }
}

static void print_trigger_stats(void) {
    const trigger_stats_t *stats = trigger_get_stats();
    unsigned long expected = stats->periodic_expected + stats->change_expected +
                             stats->range_expected + stats->interrupt_expected;
    unsigned long detected = stats->periodic_detected + stats->change_detected +
                             stats->range_detected + stats->interrupt_detected;
    double accuracy = expected == 0 ? 100.0 : (100.0 * (double)detected / expected);

    printf("trigger-test samples=%lu sent=%lu accuracy=%.2f%% "
           "expected=%lu detected=%lu missed=%lu\n",
           stats->samples, stats->sent_samples, accuracy, expected, detected,
           expected - detected);
    printf("  periodic=%lu/%lu change=%lu/%lu range=%lu/%lu interrupt=%lu/%lu\n",
           stats->periodic_detected, stats->periodic_expected,
           stats->change_detected, stats->change_expected,
           stats->range_detected, stats->range_expected,
           stats->interrupt_detected, stats->interrupt_expected);
}

int app_run(int argc, char **argv) {
    simulator_mode_t mode = SIM_MODE_NORMAL;
    unsigned int interval = 10;
    unsigned long test_samples = 0;
    int once = 0;
    int i;
    unsigned long sample_count = 0;

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--once") == 0) {
            once = 1;
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = parse_mode(argv[++i]);
        } else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) {
            interval = (unsigned int)strtoul(argv[++i], NULL, 10);
            if (interval == 0) interval = 1;
        } else if (strcmp(argv[i], "--trigger-test") == 0 && i + 1 < argc) {
            test_samples = strtoul(argv[++i], NULL, 10);
            if (test_samples == 0) {
                usage(argv[0]);
                return 2;
            }
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    simulator_init(mode);
    trigger_init(interval);
    if (getenv("MQTT_RECEIVE") != NULL &&
        strcmp(getenv("MQTT_RECEIVE"), "1") == 0 &&
        interaction_start() != 0) {
        return 1;
    }

    while ((once && sample_count < 1) ||
           (!once && (test_samples == 0 || sample_count < test_samples))) {
        sensor_data_t data;
        char payload[2048];
        unsigned int reasons = 0;
        unsigned int metric_mask;
        int length;
        int should_send;

        if (simulator_next(&data) != 0) {
            fprintf(stderr, "failed to read CPU temperature\n");
            return 1;
        }

        metric_mask = trigger_evaluate(&data, &reasons);
        if (metric_mask == 0) {
            printf("sequence=%lu trigger=none\n", data.sequence);
        } else {
            length = payload_build(payload, sizeof(payload), &data, metric_mask);
            if (length < 0) {
                fprintf(stderr, "failed to build payload\n");
                return 1;
            }
            print_reason_names(reasons);
            printf(" sequence=%lu %s\n", data.sequence, payload);
            fflush(stdout);

            should_send = !test_samples && getenv("MQTT_SEND") != NULL &&
                          strcmp(getenv("MQTT_SEND"), "1") == 0;
            if (should_send && transport_publish(payload, (size_t)length) != 0) {
                fprintf(stderr, "failed to publish payload\n");
                return 1;
            }
        }

        sample_count++;
        if (test_samples == 0 && !once) sleep(interval);
    }

    if (test_samples != 0) {
        print_trigger_stats();
    }
    return 0;
}
