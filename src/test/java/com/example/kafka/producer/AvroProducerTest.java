package com.example.kafka.producer;

import avro.Customer;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.List;

import static com.example.kafka.MainApp.getCustomers;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AvroProducerTest {

    private MockProducer<String, Customer> mockProducer;
    private MockSchemaRegistryClient mockSchemaRegistryClient;

    private AvroProducer avroProducer;

    @BeforeEach
    public void setup() throws NoSuchFieldException {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        mockProducer = new MockProducer<>(true, new StringSerializer(), getAvroSerializer());
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

    private <Customer> Serializer<Customer> getAvroSerializer() {
        Serializer<Customer> serializer =
                (Serializer) new KafkaAvroSerializer(mockSchemaRegistryClient);
        return serializer;
    }

}
