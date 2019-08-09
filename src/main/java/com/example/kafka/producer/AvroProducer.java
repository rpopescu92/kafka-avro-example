package com.example.kafka.producer;

import avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.Stream;

public class AvroProducer {

    private ProducerProperties producerProperties;
    private Producer<String, Customer> producer;

    public AvroProducer(ProducerProperties producerProperties) {
        this.producerProperties = producerProperties;
        producer = new KafkaProducer<>(getProperties());
    }

    public void sendEvent(Stream<Customer> customers) {
        customers.forEach(user -> {
            ProducerRecord record =
                 new ProducerRecord<String, Customer>(producerProperties.getTopic(), user);

                producer.send(record);
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

        return properties;

    }
}
