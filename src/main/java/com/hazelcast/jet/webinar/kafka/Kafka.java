package com.hazelcast.jet.webinar.kafka;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.webinar.KafkaProps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.webinar.KafkaProps.producerProperties;

public class Kafka {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        EXECUTOR.submit(Kafka::publish).get();

        JetInstance jet = Jet.newJetInstance();

        Pipeline p = Pipeline.create();

        p.drawFrom(KafkaSources.kafka(KafkaProps.brokerProperties(), "t1"))
                .drainTo(Sinks.map("records"));

        jet.newJob(p);

        while (true) {
            System.out.println("Size of map is " + jet.getMap("records").size());
            System.out.println(jet.getMap("records").get("t1-100"));
            Thread.sleep(5000);
        }
    }

    private static void publish() {
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProperties())) {
            for (int i = 1; i <= 1_000_000; i++) {
                producer.send(new ProducerRecord<>("t1", "t1-" + i, i));
                if (i % 100_000 == 0) {
                    System.out.println("Published " + i + " messages to topic t1");

                }
            }
        }
    }
}

