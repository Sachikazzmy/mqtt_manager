#include "trigger.h"

#include <math.h>
#include <string.h>

typedef struct {
    unsigned int temperature_period;
    unsigned int auxiliary_period;
    unsigned long samples;
    double last_pressure_sent;
    int pressure_initialized;
    int current_in_alarm;
    trigger_stats_t stats;
} trigger_state_t;

static trigger_state_t state;

static unsigned int samples_for_seconds(unsigned int seconds,
                                        unsigned int interval) {
    unsigned int samples = (seconds + interval - 1u) / interval;
    return samples == 0 ? 1 : samples;
}

void trigger_init(unsigned int sample_interval_seconds) {
    memset(&state, 0, sizeof(state));
    if (sample_interval_seconds == 0) {
        sample_interval_seconds = 1;
    }

    state.temperature_period = samples_for_seconds(30,
                                                   sample_interval_seconds);
    state.auxiliary_period = samples_for_seconds(60,
                                                 sample_interval_seconds);
}

unsigned int trigger_evaluate(const sensor_data_t *data, unsigned int *reasons) {
    unsigned int mask = 0;
    unsigned int reason_mask = 0;
    int periodic_due;
    int auxiliary_due;
    int current_in_alarm;

    if (data == NULL) {
        return 0;
    }

    state.samples++;
    state.stats.samples = state.samples;
    periodic_due = state.samples == 1 ||
                   state.samples % state.temperature_period == 0;
    auxiliary_due = state.samples == 1 ||
                    state.samples % state.auxiliary_period == 0;

    if (periodic_due) {
        mask |= METRIC_TEMPERATURE;
        reason_mask |= TRIGGER_PERIODIC;
        state.stats.periodic_expected++;
        state.stats.periodic_detected++;
    }

    if (!state.pressure_initialized ||
        fabs(data->pressure - state.last_pressure_sent) >= 0.5 ||
        auxiliary_due) {
        mask |= METRIC_PRESSURE;
        if (!state.pressure_initialized ||
            fabs(data->pressure - state.last_pressure_sent) >= 0.5) {
            reason_mask |= TRIGGER_CHANGE;
            state.stats.change_expected++;
            state.stats.change_detected++;
        }
        if (auxiliary_due) {
            reason_mask |= TRIGGER_PERIODIC;
        }
        state.last_pressure_sent = data->pressure;
        state.pressure_initialized = 1;
    }

    current_in_alarm = data->current < 1.0 || data->current > 3.0;
    if (current_in_alarm != state.current_in_alarm) {
        mask |= METRIC_CURRENT;
        reason_mask |= TRIGGER_RANGE;
        state.stats.range_expected++;
        state.stats.range_detected++;
        state.current_in_alarm = current_in_alarm;
    }
    if (auxiliary_due) {
        mask |= METRIC_CURRENT;
        reason_mask |= TRIGGER_PERIODIC;
    }

    if ((data->event_flags & SENSOR_EVENT_INTERRUPT) != 0) {
        mask |= METRIC_TEMPERATURE;
        reason_mask |= TRIGGER_INTERRUPT;
        state.stats.interrupt_expected++;
        state.stats.interrupt_detected++;
    }

    if (mask != 0) {
        state.stats.sent_samples++;
    }
    if (reasons != NULL) {
        *reasons = reason_mask;
    }
    return mask;
}

const trigger_stats_t *trigger_get_stats(void) {
    return &state.stats;
}
