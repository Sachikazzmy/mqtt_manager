#ifndef TRANSPORT_H
#define TRANSPORT_H

#include <stddef.h>

int transport_publish(const char *payload, size_t payload_length);

#endif
