package com.example.kafka.producer;

import avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.Stream;

public class AvroProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);

    private ProducerProperties producerProperties;
    private Producer<String, Customer> producer;

    public AvroProducer(ProducerProperties producerProperties) {
        this.producerProperties = producerProperties;
        producer = new KafkaProducer<>(getProperties());
    }

    public void sendEvent(Stream<Customer> customers) {
        customers.forEach(customer -> {
            ProducerRecord<String, Customer> record =
                 new ProducerRecord<>(producerProperties.getTopic(), customer);

                producer.send(record);
                LOGGER.info("Sent record {}", record.toString());
        });
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                producerProperties.getBootstrapServers());
        properties.put("schema.registry.url",
                producerProperties.getSchemaRegistry());
        properties.put("key.serializer", StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "customer12");

        return properties;

    }
}
