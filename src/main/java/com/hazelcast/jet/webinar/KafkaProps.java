package com.hazelcast.jet.webinar;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProps {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";


    public static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length;) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }

    public static Properties producerProperties() {
        return props(
                "bootstrap.servers", "localhost:9092",
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", IntegerSerializer.class.getName());
    }

    public static Properties brokerProperties() {
        return props(
                "bootstrap.servers", BOOTSTRAP_SERVERS,
                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                "value.deserializer", IntegerDeserializer.class.getCanonicalName(),
                "auto.offset.reset", "earliest");
    }
}
