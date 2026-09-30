#include "gateway_app.h"

#include "payload.h"
#include "transport.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int app_run(void) {
    char payload[2048];
    const char *send_enabled = getenv("MQTT_SEND");

    int length = payload_build(payload, sizeof(payload));
    if (length < 0) {
        fprintf(stderr, "failed to build payload\n");
        return 1;
    }

    printf("%s\n", payload);

    if (send_enabled != NULL && strcmp(send_enabled, "1") == 0) {
        if (transport_publish(payload, (size_t)length) != 0) {
            fprintf(stderr, "failed to publish payload\n");
            return 1;
        }
        printf("payload published\n");
    }

    return 0;
}
