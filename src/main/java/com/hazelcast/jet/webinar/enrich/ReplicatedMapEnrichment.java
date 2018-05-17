package com.hazelcast.jet.webinar.enrich;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.ContextFactories;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.webinar.KafkaProps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.webinar.KafkaProps.*;


public class ReplicatedMapEnrichment {

    private static final String INSTRUMENTS = "instruments";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance instance = Jet.newJetInstance();
        try {

            Map<String, Instrument> instruments = instance.getHazelcastInstance().getReplicatedMap(INSTRUMENTS);

            instruments.put("AAAP", new Instrument("AAAP", "Advanced Accelerator Applications S.A."));
            instruments.put("BABY", new Instrument("BABY", "Natus Medical Incorporated"));
            instruments.put("CA", new Instrument("CA", "CA Inc."));

            Pipeline pipeline = Pipeline.create();

            pipeline.drawFrom(KafkaSources.<String, Integer>kafka(brokerProperties(), "trades"))
                    .mapUsingContext(ContextFactories.<String, Instrument>replicatedMapContext(INSTRUMENTS),
                            (map, e) -> {
                                String ticker = e.getKey();
                                return new Trade(ticker, map.get(ticker).securityName, e.getValue());
                            })
                    .drainTo(Sinks.logger());

            Job job = instance.newJob(pipeline);

            try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProperties())) {
                producer.send(new ProducerRecord<>("trades", "AAAP", 100));
                producer.send(new ProducerRecord<>("trades", "BABY", 1000));
                producer.send(new ProducerRecord<>("trades", "CA", 150));
            }

            job.join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
