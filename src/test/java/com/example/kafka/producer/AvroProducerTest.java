package com.example.kafka.producer;

import avro.Customer;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AvroProducerTest {

    private MockProducer<String, Customer> mockProducer;
    private MockSchemaRegistryClient mockSchemaRegistryClient;

    private AvroProducer avroProducer;

    @BeforeEach
    public void setup() throws NoSuchFieldException {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        mockProducer = new MockProducer<>(true, new StringSerializer(), getSerializer());
        ProducerProperties kafkaConfig = new ProducerProperties();
        avroProducer = new AvroProducer(kafkaConfig);
        FieldSetter.setField(avroProducer, avroProducer.getClass().getDeclaredField("producer"), mockProducer);

    }

    @Test
    public void shouldSendEventsToTopic() {
        avroProducer.sendEvent(getCustomers());
        final List<ProducerRecord<String, Customer>> producerRecords = mockProducer.history();

        assertThat(producerRecords.size(), equalTo(2));
        assertThat(producerRecords.get(0).value().getId(), equalTo(1));
        assertThat(producerRecords.get(0).value().getEmail(), equalTo("customer1@mail.com"));
        assertThat(producerRecords.get(1).value().getId(), equalTo(2));
        assertThat(producerRecords.get(1).value().getEmail(), equalTo("customer2@mail.com"));
    }

    public Stream<Customer> getCustomers() {
        Customer customer1 = Customer.newBuilder()
                                    .setEmail("customer1@mail.com")
                                    .setFirstName("Customer 1")
                                    .setLastName("Test")
                                    .setId(1)
                                    .build();
        Customer customer2 = Customer.newBuilder()
                .setEmail("customer2@mail.com")
                .setFirstName("Customer 2")
                .setLastName("Test")
                .setId(2)
                .build();

        return Stream.of(customer1, customer2);
    }

    private <Customer> Serializer<Customer> getSerializer() {
        Map<String, Object> map = new HashMap<>();
        map.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8089");
        Serializer<Customer> serializer = (Serializer) new KafkaAvroSerializer(mockSchemaRegistryClient);
        return serializer;
    }

}
