package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SpecificRecordBase> snapshotConsumer;
    private final ScenarioAnalyzer scenarioAnalyzer;

    @Value("${analyzer.kafka.topics.snapshots}")
    private String snapshotsTopic;

    public void start() {
        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("SnapshotProcessor subscribed to topic: {}", snapshotsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records =
                        snapshotConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    if (record.value() instanceof SensorsSnapshotAvro snapshot) {
                        log.info("=== SNAPSHOT RECEIVED ===");
                        log.info("hubId: {}", snapshot.getHubId());
                        log.info("timestamp: {}", snapshot.getTimestamp());
                        log.info("sensors count: {}", snapshot.getSensorsState().size());
                        log.info("sensors: {}", snapshot.getSensorsState().keySet());

                        scenarioAnalyzer.analyzeSnapshot(snapshot);
                    }
                }
                snapshotConsumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor received shutdown signal");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            try {
                snapshotConsumer.commitSync();
            } finally {
                snapshotConsumer.close();
                log.info("SnapshotProcessor closed");
            }
        }
    }
}