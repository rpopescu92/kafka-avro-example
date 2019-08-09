package com.example.kafka.producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerProperties {

    public static final String BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers";
    public static final String SCHEMA_REGISTRY_KEY = "kafka.schema.registry.url";
    public static final String TOPIC_KEY = "kafka.topic";
    public static final String CLIENT_ID_KEY = "kafka.clientid";

    private Properties kafkaProps;

    public ProducerProperties() {
        try {
            String fileProps = ProducerProperties.class.getResource("/application.properties").getPath();
            kafkaProps = new Properties();

            kafkaProps.load(new FileInputStream(fileProps));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getBootstrapServers() {
        return kafkaProps.getProperty(BOOTSTRAP_SERVERS_KEY);
    }

    public String getSchemaRegistry() {
        return kafkaProps.getProperty(SCHEMA_REGISTRY_KEY);
    }

    public String getTopic() {
        return kafkaProps.getProperty(TOPIC_KEY);
    }

    public String getClientId() {
        return kafkaProps.getProperty(CLIENT_ID_KEY);
    }

    public static void main(String[]args) {
        ProducerProperties producerProperties = new ProducerProperties();

    }
}
