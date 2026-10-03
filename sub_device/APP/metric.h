#ifndef METRIC_H
#define METRIC_H

enum {
    METRIC_TEMPERATURE = 1u << 0,
    METRIC_PRESSURE = 1u << 1,
    METRIC_CURRENT = 1u << 2
};

enum {
    SENSOR_EVENT_INTERRUPT = 1u << 0
};

typedef struct {
    double temperature;
    double pressure;
    double current;
    unsigned long sequence;
    unsigned int event_flags;
} sensor_data_t;

#endif
