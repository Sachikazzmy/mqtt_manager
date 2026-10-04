#include "simulator.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

typedef struct {
    simulator_mode_t mode;
    unsigned long sequence;
    double pressure;
    double current;
    unsigned int random_state;
    int thermal_zone;
    int pressure_override;
    int current_override;
} simulator_state_t;

static simulator_state_t state;

static unsigned int next_random(void) {
    state.random_state ^= state.random_state << 13;
    state.random_state ^= state.random_state >> 17;
    state.random_state ^= state.random_state << 5;
    return state.random_state;
}

static double noise(double amplitude) {
    double normalized = (double)(next_random() % 10001) / 10000.0;
    return (normalized * 2.0 - 1.0) * amplitude;
}

static int find_thermal_zone(void) {
    char type_path[128];
    char type[64];
    int zone;

    for (zone = 0; zone < 32; ++zone) {
        snprintf(type_path, sizeof(type_path),
                 "/sys/class/thermal/thermal_zone%d/type", zone);

        FILE *file = fopen(type_path, "r");
        if (file == NULL) {
            continue;
        }

        if (fgets(type, sizeof(type), file) != NULL) {
            fclose(file);
            if (strstr(type, "cpu") != NULL || strstr(type, "soc") != NULL) {
                return zone;
            }
        } else {
            fclose(file);
        }
    }

    return 0;
}

static int read_cpu_temperature(int zone, double *temperature) {
    char temp_path[128];
    long raw;
    FILE *file;

    snprintf(temp_path, sizeof(temp_path),
             "/sys/class/thermal/thermal_zone%d/temp", zone);

    file = fopen(temp_path, "r");
    if (file == NULL || fscanf(file, "%ld", &raw) != 1) {
        if (file != NULL) {
            fclose(file);
        }
        return -1;
    }
    fclose(file);

    *temperature = raw > 1000 ? (double)raw / 1000.0 : (double)raw;
    return 0;
}

int simulator_init(simulator_mode_t mode) {
    state.mode = mode;
    state.sequence = 0;
    state.pressure = 101.3;
    state.current = 2.5;
    state.random_state = (unsigned int)time(NULL) ^ (unsigned int)getpid();
    if (state.random_state == 0) {
        state.random_state = 0x13572468u;
    }
    state.thermal_zone = find_thermal_zone();
    return 0;
}

int simulator_next(sensor_data_t *data) {
    double temperature;

    if (data == NULL || read_cpu_temperature(state.thermal_zone, &temperature) != 0) {
        return -1;
    }

    state.sequence++;
    if (!state.pressure_override) state.pressure += noise(0.08);
    if (!state.current_override) state.current += noise(0.04);

    if (!state.pressure_override) {
        if (state.pressure < 100.0) state.pressure = 100.0;
        if (state.pressure > 103.0) state.pressure = 103.0;
    }
    if (!state.current_override) {
        if (state.current < 1.0) state.current = 1.0;
        if (state.current > 3.0) state.current = 3.0;
    }

    if (state.mode == SIM_MODE_TEST) {
        if (state.sequence >= 20 && state.sequence < 30) {
            state.pressure = 120.0;
        } else if (state.sequence >= 50 && state.sequence < 60) {
            state.current = 12.0;
        } else if (state.sequence >= 80 && state.sequence < 90) {
            state.pressure = 88.0;
        }
    } else if (state.mode == SIM_MODE_RANDOM) {
        if (state.sequence % 97 == 0) {
            state.pressure = 120.0;
        } else if (state.sequence % 131 == 0) {
            state.current = 12.0;
        }
    }

    data->temperature = temperature;
    data->pressure = state.pressure;
    data->current = state.current;
    data->sequence = state.sequence;
    data->event_flags = 0;
    if (state.mode == SIM_MODE_TEST && state.sequence % 37 == 0) {
        data->event_flags |= SENSOR_EVENT_INTERRUPT;
    } else if (state.mode == SIM_MODE_RANDOM && state.sequence % 113 == 0) {
        data->event_flags |= SENSOR_EVENT_INTERRUPT;
    }
    return 0;
}

int simulator_set_metric(const char *metric_key, double value) {
    if (metric_key == NULL) return -1;
    if ((strcmp(metric_key, "pressure") == 0 || strcmp(metric_key, "segment-2") == 0)) {
        state.pressure = value;
        state.pressure_override = 1;
        return 0;
    }
    if ((strcmp(metric_key, "current") == 0 || strcmp(metric_key, "segment-3") == 0)) {
        state.current = value;
        state.current_override = 1;
        return 0;
    }
    return -1;
}

int simulator_clear_metric(const char *metric_key) {
    if (metric_key == NULL) return -1;
    if ((strcmp(metric_key, "pressure") == 0 || strcmp(metric_key, "segment-2") == 0)) {
        state.pressure = 101.3;
        state.pressure_override = 0;
        return 0;
    }
    if ((strcmp(metric_key, "current") == 0 || strcmp(metric_key, "segment-3") == 0)) {
        state.current = 2.5;
        state.current_override = 0;
        return 0;
    }
    return -1;
}
