#ifndef PAYLOAD_H
#define PAYLOAD_H

#include <stddef.h>

#include "metric.h"

int payload_build(char *buffer, size_t buffer_size,
                  const sensor_data_t *data, unsigned int metric_mask);

#endif
