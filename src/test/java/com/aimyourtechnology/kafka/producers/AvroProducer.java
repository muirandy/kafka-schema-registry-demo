package com.aimyourtechnology.kafka.producers;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class AvroProducer {
    private static final String TOPIC = "avrotopic1";
    private static final String SUBJECT = TOPIC + "-value";
    private static final int SUBJECT_VERSION_1 = 1;

    private String[] args;

    private Schema schema;

    public AvroProducer(String[] args) {
        this.args = args;
    }

    public static void main(final String[] args) {
        AvroProducer avroProducer = new AvroProducer(args);
        avroProducer.execute();
    }

    private void execute() {
        obtainSchema();
        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(getKafkaProperties())) {
            sendOrders(producer);
        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void obtainSchema() {
        try {
            readSchemaFromSchemaRegistry();
        } catch (IOException e) {
            System.err.println(e);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private void readSchemaFromSchemaRegistry() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(SUBJECT, SUBJECT_VERSION_1);
        schema = schemaRegistryClient.getById(schemaMetadata.getId());
    }

    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        return props;
    }

    private String getSchemaRegistryUrl() {
        return "http://localhost:8081";
    }

    private void sendOrders(KafkaProducer<String, GenericRecord> producer) throws InterruptedException {
        for (long i = 0; i < 10; i++)
            sendOrder(producer, "id" + i);

        producer.flush();
        System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
    }

    private void sendOrder(KafkaProducer<String, GenericRecord> producer, String orderId) throws InterruptedException {
        GenericRecord payment = new GenericData.Record(schema);
        payment.put("id", orderId);
        payment.put("amount", 1000.00d);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TOPIC, orderId, payment);
        producer.send(record);
        Thread.sleep(1000L);
    }

    private class SchemaRegistryIoException extends RuntimeException {
        public SchemaRegistryIoException(IOException e) {
        }
    }

    private class SchemaRegistryClientException extends RuntimeException {
        public SchemaRegistryClientException(RestClientException e) {
        }
    }
}
