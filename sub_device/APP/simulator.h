#ifndef SIMULATOR_H
#define SIMULATOR_H

#include "metric.h"

typedef enum {
    SIM_MODE_NORMAL,
    SIM_MODE_TEST,
    SIM_MODE_RANDOM
} simulator_mode_t;

int simulator_init(simulator_mode_t mode);
int simulator_next(sensor_data_t *data);
int simulator_set_metric(const char *metric_key, double value);
int simulator_clear_metric(const char *metric_key);

#endif
