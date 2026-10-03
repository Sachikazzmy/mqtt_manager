#include "transport.h"

#include <MQTTClient.h>

#include <stdio.h>
#include <stdlib.h>

#define MQTT_URI "wss://mqtt.web4sachika.asia:443/mqtt"
#define MQTT_TOPIC "factory/linux-01/telemetry"
#define MQTT_CLIENT_ID "sensor-linux-01-telemetry"

static const char *default_ca_file(void) {
#ifdef _WIN32
    return "C:/msys64/ucrt64/etc/ssl/certs/ca-bundle.crt";
#else
    return "/etc/ssl/certs/ca-certificates.crt";
#endif
}

int transport_publish(const char *payload, size_t payload_length) {
    const char *username = getenv("MQTT_USER");
    const char *password = getenv("MQTT_KEY");
    const char *ca_file = getenv("MQTT_CA_FILE");
    MQTTClient client = NULL;
    MQTTClient_connectOptions connect_options = MQTTClient_connectOptions_initializer_ws;
    MQTTClient_SSLOptions ssl_options = MQTTClient_SSLOptions_initializer;
    MQTTClient_message message = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;

    if (username == NULL || password == NULL) {
        fprintf(stderr, "MQTT_USER and MQTT_KEY must be set\n");
        return -1;
    }

    if (ca_file == NULL || ca_file[0] == '\0') {
        ca_file = default_ca_file();
    }

    rc = MQTTClient_create(
        &client,
        MQTT_URI,
        MQTT_CLIENT_ID,
        MQTTCLIENT_PERSISTENCE_NONE,
        NULL
    );
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "MQTTClient_create failed: %d\n", rc);
        return -1;
    }

    ssl_options.trustStore = ca_file;
    ssl_options.enableServerCertAuth = 1;
    connect_options.username = username;
    connect_options.password = password;
    connect_options.ssl = &ssl_options;
    connect_options.keepAliveInterval = 60;
    connect_options.cleansession = 1;

    rc = MQTTClient_connect(client, &connect_options);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "MQTTClient_connect failed: %d\n", rc);
        MQTTClient_destroy(&client);
        return -1;
    }

    message.payload = (void *)payload;
    message.payloadlen = (int)payload_length;
    message.qos = 1;
    message.retained = 0;

    rc = MQTTClient_publishMessage(client, MQTT_TOPIC, &message, &token);
    if (rc == MQTTCLIENT_SUCCESS) {
        rc = MQTTClient_waitForCompletion(client, token, 10000L);
    }

    MQTTClient_disconnect(client, 5000);
    MQTTClient_destroy(&client);

    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "MQTT publish failed: %d\n", rc);
        return -1;
    }

    return 0;
}
