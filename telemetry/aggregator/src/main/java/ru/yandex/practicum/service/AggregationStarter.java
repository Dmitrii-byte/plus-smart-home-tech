package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final Consumer<String, SpecificRecordBase> consumer;
    private final Producer<String, SpecificRecordBase> producer;

    @Value("${kafka.topic.sensors}")
    private String inputTopic;

    @Value("${kafka.topic.snapshots}")
    private String outputTopic;

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public void start() {
        try {
            consumer.subscribe(java.util.List.of(inputTopic));
            log.info("Subscribed to topic: {}", inputTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    SpecificRecordBase value = record.value();
                    if (value instanceof SensorEventAvro event) {
                        log.debug("Received sensor event: id={}, hubId={}, timestamp={}",
                                event.getId(), event.getHubId(), event.getTimestamp());

                        Optional<SensorsSnapshotAvro> updatedSnapshot = updateState(event);

                        if (updatedSnapshot.isPresent()) {
                            SensorsSnapshotAvro snapshot = updatedSnapshot.get();

                            log.info("=== SENDING SNAPSHOT ===");
                            log.info("hubId: {}", snapshot.getHubId());
                            log.info("timestamp: {}", snapshot.getTimestamp());
                            log.info("sensorsState: {}",
                                    snapshot.getSensorsState().keySet());

                            ProducerRecord<String, SpecificRecordBase> producerRecord =
                                    new ProducerRecord<>(
                                            outputTopic,
                                            snapshot.getHubId(),
                                            snapshot
                                    );

                            producer.send(producerRecord, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("Failed to send snapshot: ", exception);
                                } else {
                                    log.info("Snapshot sent successfully: partition={}, offset={}",
                                            metadata.partition(), metadata.offset());
                                }
                            });
                        }
                    } else {
                        log.warn("Received message of unknown type: {}",
                                value != null ? value.getClass().getName() : "null");
                    }
                }
                consumer.commitSync();
                log.debug("Committed offsets");
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal");
        } catch (Exception e) {
            log.error("Error while processing sensor events", e);
        } finally {
            try {
                log.info("Flushing producer and committing final offsets");
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Closing consumer and producer");
                consumer.close();
                producer.close();
            }
        }
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant eventTimestamp = event.getTimestamp();

        SensorsSnapshotAvro snapshot = snapshots.get(hubId);
        if (snapshot == null) {
            log.info("Creating new snapshot for hub: {}", hubId);

            Map<String, SensorStateAvro> sensorsState = new HashMap<>();
            SensorStateAvro sensorState = SensorStateAvro.newBuilder()
                    .setTimestamp(eventTimestamp)
                    .setData(event.getPayload())
                    .build();
            sensorsState.put(sensorId, sensorState);

            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(hubId)
                    .setTimestamp(eventTimestamp)
                    .setSensorsState(sensorsState)
                    .build();
            snapshots.put(hubId, snapshot);

            log.info("Created new snapshot for hub: {} with sensor: {}", hubId, sensorId);
            return Optional.of(snapshot);
        }

        SensorStateAvro oldState = snapshot.getSensorsState().get(sensorId);

        if (oldState != null && oldState.getTimestamp().isAfter(eventTimestamp)) {
            log.debug("Ignoring outdated event for sensor: {}. Event timestamp: {}, Current timestamp: {}",
                    sensorId, eventTimestamp, oldState.getTimestamp());
            return Optional.empty();
        }

        if (oldState != null && oldState.getData().equals(event.getPayload())) {
            log.debug("Ignoring duplicate event for sensor: {}. Data hasn't changed", sensorId);
            return Optional.empty();
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(sensorId, newState);
        snapshot.setTimestamp(eventTimestamp);

        log.info("Updated sensor: {} in snapshot for hub: {} at timestamp: {}",
                sensorId, hubId, eventTimestamp);

        return Optional.of(snapshot);
    }
}