package ru.practirum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.practirum.collector.model.hub.HubEvent;
import ru.practirum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectorService {
    private final KafkaProducer<String, Object> kafkaProducer;
    private final SensorEventConverter sensorEventConverter;
    private final HubEventConverter hubEventConverter;

    @Value("${kafka.topic.sensors}")
    private String sensorsTopic;

    @Value("${kafka.topic.hubs}")
    private String hubsTopic;

    public void processSensorEvent(SensorEvent event) {
        try {
            SensorEventAvro avroEvent = sensorEventConverter.convert(event);
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                    sensorsTopic,
                    event.getHubId(),
                    avroEvent
            );

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.debug("Sensor event sent to Kafka: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to send sensor event to Kafka", exception);
                }
            });
        } catch (Exception e) {
            log.error("Error processing sensor event: id={}, hubId={}", event.getId(), event.getHubId(), e);
            throw new RuntimeException("Failed to process sensor event", e);
        }
    }

    public void processHubEvent(HubEvent event) {
        try {
            HubEventAvro avroEvent = hubEventConverter.convert(event);
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                    hubsTopic,
                    event.getHubId(),
                    avroEvent
            );

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.debug("Hub event sent to Kafka: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to send hub event to Kafka", exception);
                }
            });
        } catch (Exception e) {
            log.error("Error processing hub event: hubId={}, type={}", event.getHubId(), event.getType(), e);
            throw new RuntimeException("Failed to process hub event", e);
        }
    }
}