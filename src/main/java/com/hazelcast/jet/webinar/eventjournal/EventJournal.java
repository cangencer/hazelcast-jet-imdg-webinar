package com.hazelcast.jet.webinar.eventjournal;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

public class EventJournal {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("mapA"));
        JetInstance instance = Jet.newJetInstance(config);

        IMapJet<String, Integer> mapA = instance.getMap("mapA");
        IMapJet<String, Integer> mapB = instance.getMap("mapB");

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.mapJournal("mapA", JournalInitialPosition.START_FROM_OLDEST))
                .drainTo(Sinks.map("mapB"));

        instance.newJob(pipeline);

        mapA.put("keyA", 1);
        mapA.put("keyB", 2);

        while (true) {
            System.out.println(mapB.entrySet());
            Thread.sleep(5000);
        }
    }
}
