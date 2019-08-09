package com.example.kafka;

import avro.Customer;
import com.example.kafka.producer.AvroProducer;
import com.example.kafka.producer.ProducerProperties;

import java.util.stream.Stream;

public class MainApp {


    public static void main(String[] args) {

        ProducerProperties producerProperties = new ProducerProperties();
        AvroProducer avroProducer = new AvroProducer(producerProperties);
        avroProducer.sendEvent(getCustomers());
    }

    public static Stream<Customer> getCustomers() {
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
}
