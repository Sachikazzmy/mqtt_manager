#ifndef TRIGGER_H
#define TRIGGER_H

#include "metric.h"

typedef enum {
    TRIGGER_PERIODIC = 1u << 0,
    TRIGGER_CHANGE = 1u << 1,
    TRIGGER_RANGE = 1u << 2,
    TRIGGER_INTERRUPT = 1u << 3
} trigger_reason_t;

typedef struct {
    unsigned long samples;
    unsigned long sent_samples;
    unsigned long periodic_expected;
    unsigned long periodic_detected;
    unsigned long change_expected;
    unsigned long change_detected;
    unsigned long range_expected;
    unsigned long range_detected;
    unsigned long interrupt_expected;
    unsigned long interrupt_detected;
} trigger_stats_t;

void trigger_init(unsigned int sample_interval_seconds);
unsigned int trigger_evaluate(const sensor_data_t *data, unsigned int *reasons);
const trigger_stats_t *trigger_get_stats(void);

#endif
