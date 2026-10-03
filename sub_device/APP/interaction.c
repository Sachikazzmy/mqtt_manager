#include "interaction.h"
#include "simulator.h"
#include <MQTTClient.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MQTT_URI "wss://mqtt.web4sachika.asia:443/mqtt"
#define MQTT_CLIENT_ID "sensor-linux-01-command"
#define MQTT_COMMAND_TOPIC "factory/linux-01/command"
#define MQTT_RESULT_TOPIC "factory/linux-01/command_result"
#define MAX_COMMAND_SIZE 4096

typedef struct { MQTTClient client; volatile int lost; } interaction_context_t;

static const char *default_ca_file(void) {
#ifdef _WIN32
    return "C:/msys64/ucrt64/etc/ssl/certs/ca-bundle.crt";
#else
    return "/etc/ssl/certs/ca-certificates.crt";
#endif
}

static const char *find_field(const char *json, const char *key) {
    static char needle[64];
    int length = snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *start;
    if (length < 0 || (size_t)length >= sizeof(needle)) return NULL;
    start = strstr(json, needle);
    if (start == NULL) return NULL;
    start = strchr(start + length, ':');
    if (start == NULL) return NULL;
    start++;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    return start;
}

static int json_string_field(const char *json, const char *key,
                             char *output, size_t output_size) {
    const char *start = find_field(json, key);
    const char *end;
    if (start == NULL || *start != '"') return -1;
    start++;
    end = strchr(start, '"');
    if (end == NULL || (size_t)(end - start) >= output_size) return -1;
    memcpy(output, start, (size_t)(end - start));
    output[end - start] = '\0';
    return 0;
}

static int json_double_field(const char *json, const char *key, double *value) {
    const char *start = find_field(json, key);
    char *end;
    if (start == NULL || *start == '"') return -1;
    *value = strtod(start, &end);
    if (end == start || !isfinite(*value)) return -1;
    return 0;
}

static void publish_result(interaction_context_t *context,
                           const char *command_id, const char *status,
                           const char *metric_key, double value) {
    char response[512];
    MQTTClient_message message = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int length = snprintf(response, sizeof(response),
        "{\"version\":\"1\",\"command_id\":\"%s\","
        "\"status\":\"%s\",\"metric_key\":\"%s\","
        "\"value\":%.2f}", command_id, status,
        metric_key == NULL ? "" : metric_key, value);
    if (length < 0 || (size_t)length >= sizeof(response)) return;
    message.payload = response;
    message.payloadlen = length;
    message.qos = 1;
    message.retained = 0;
    MQTTClient_publishMessage(context->client, MQTT_RESULT_TOPIC, &message, &token);
}

static int message_arrived(void *argument, char *topic_name, int topic_length,
                           MQTTClient_message *message) {
    interaction_context_t *context = argument;
    char command[MAX_COMMAND_SIZE];
    char version[8];
    char command_id[128];
    char action[32];
    char metric_key[32];
    double value;

    (void)topic_name;
    (void)topic_length;
    if (message == NULL || message->payloadlen <= 0 ||
        message->payloadlen >= MAX_COMMAND_SIZE) {
        MQTTClient_freeMessage(&message);
        MQTTClient_free(topic_name);
        return 1;
    }
    memcpy(command, message->payload, (size_t)message->payloadlen);
    command[message->payloadlen] = '\0';

    if (json_string_field(command, "version", version, sizeof(version)) != 0 ||
        strcmp(version, "1") != 0 ||
        json_string_field(command, "command_id", command_id, sizeof(command_id)) != 0 ||
        json_string_field(command, "action", action, sizeof(action)) != 0 ||
        json_string_field(command, "metric_key", metric_key, sizeof(metric_key)) != 0) {
        publish_result(context, "unknown", "rejected", NULL, 0.0);
    } else if (strcmp(action, "set_metric") == 0 &&
               json_double_field(command, "value", &value) == 0 &&
               simulator_set_metric(metric_key, value) == 0) {
        publish_result(context, command_id, "applied", metric_key, value);
    } else if (strcmp(action, "clear_override") == 0 &&
               simulator_clear_metric(metric_key) == 0) {
        publish_result(context, command_id, "applied", metric_key, 0.0);
    } else {
        publish_result(context, command_id, "rejected", metric_key, 0.0);
    }

    MQTTClient_freeMessage(&message);
    MQTTClient_free(topic_name);
    return 1;
}

static void connection_lost(void *argument, char *cause) {
    interaction_context_t *context = argument;
    context->lost = 1;
    fprintf(stderr, "MQTT command connection lost: %s\n",
            cause == NULL ? "unknown" : cause);
}

static void delivery_complete(void *context, MQTTClient_deliveryToken token) {
    (void)context;
    (void)token;
}

static void *interaction_worker(void *argument) {
    const char *username = getenv("MQTT_USER");
    const char *password = getenv("MQTT_KEY");
    const char *ca_file = getenv("MQTT_CA_FILE");
    unsigned int backoff = 1;
    (void)argument;

    if (username == NULL || password == NULL) {
        fprintf(stderr, "MQTT_USER and MQTT_KEY must be set for commands\n");
        return NULL;
    }
    if (ca_file == NULL || ca_file[0] == '\0') ca_file = default_ca_file();

    while (1) {
        interaction_context_t context;
        MQTTClient_connectOptions options = MQTTClient_connectOptions_initializer_ws;
        MQTTClient_SSLOptions ssl = MQTTClient_SSLOptions_initializer;
        int rc;

        memset(&context, 0, sizeof(context));
        rc = MQTTClient_create(&context.client, MQTT_URI, MQTT_CLIENT_ID,
                               MQTTCLIENT_PERSISTENCE_NONE, NULL);
        if (rc == MQTTCLIENT_SUCCESS) {
            MQTTClient_setCallbacks(context.client, &context, connection_lost,
                                    message_arrived, delivery_complete);
            ssl.trustStore = ca_file;
            ssl.enableServerCertAuth = 1;
            options.username = username;
            options.password = password;
            options.ssl = &ssl;
            options.keepAliveInterval = 60;
            options.cleansession = 1;
            rc = MQTTClient_connect(context.client, &options);
        }
        if (rc == MQTTCLIENT_SUCCESS) {
            rc = MQTTClient_subscribe(context.client, MQTT_COMMAND_TOPIC, 1);
        }
        if (rc == MQTTCLIENT_SUCCESS) {
            fprintf(stderr, "MQTT command subscription active: %s\n",
                    MQTT_COMMAND_TOPIC);
            backoff = 1;
            while (!context.lost) {
                MQTTClient_yield();
                sleep(1);
            }
        } else {
            fprintf(stderr, "MQTT command connection failed: %d\n", rc);
        }
        MQTTClient_disconnect(context.client, 1000);
        MQTTClient_destroy(&context.client);
        sleep(backoff);
        if (backoff < 30) backoff *= 2;
        if (backoff > 30) backoff = 30;
    }
}

int interaction_start(void) {
    pthread_t thread;
    if (pthread_create(&thread, NULL, interaction_worker, NULL) != 0) {
        fprintf(stderr, "failed to start MQTT command worker\n");
        return -1;
    }
    pthread_detach(thread);
    return 0;
}
