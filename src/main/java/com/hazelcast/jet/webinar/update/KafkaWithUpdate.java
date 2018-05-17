package com.hazelcast.jet.webinar.update;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.webinar.KafkaProps.*;

public class KafkaWithUpdate {

    public static final String PRODUCTS_MAP_NAME = "products";
    public static final String PRICES_TOPIC = "product-prices";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetInstance instance = Jet.newJetInstance();
            IMapJet<String, Product> products = instance.getMap(PRODUCTS_MAP_NAME);
            createProducts(products);


            Pipeline pipeline = Pipeline.create();
            pipeline.drawFrom(KafkaSources.<String, Integer>kafka(brokerProperties(), PRICES_TOPIC))
                    .drainTo(Sinks.mapWithUpdating("products", (Product p, Map.Entry<String, Integer> e) -> {
                        p.setPrice(e.getValue());
                        return p;
                    }));

            printProducts(products);
            System.out.println("Starting job");

            // start the job
            Job job = instance.newJob(pipeline);


            try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProperties())) {
                producer.send(new ProducerRecord<>(PRICES_TOPIC, "p1", 995));
                producer.send(new ProducerRecord<>(PRICES_TOPIC, "p2", 15100));
            }

            while (true) {
                printProducts(products);
                Thread.sleep(5000);
            }
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void printProducts(IMapJet<String, Product> products) {
        for (Map.Entry<String, Product> e : products.entrySet()) {
            System.out.println(e.getKey() + ":" + e.getValue());
        }
    }

    private static void createProducts(Map<String, Product> products) {
        products.put("p1", new Product("p1", "Product 1", 1000, 100));
        products.put("p2", new Product("p2", "Product 2", 15000, 100));
        products.put("p3", new Product("p3", "Product 3", 25000, 100));
        products.put("p4", new Product("p4", "Product 4", 10000, 100));
    }
}

