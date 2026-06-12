package ru.practirum.collector.service;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.practirum.collector.exceptions.EventConversionException;
import ru.practirum.collector.exceptions.EventProcessingException;
import ru.practirum.collector.exceptions.KafkaSendException;
import ru.practirum.collector.handler.SensorEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CollectorService {

    private final KafkaProducer<String, Object> kafkaProducer;
    private final SensorEventConverter sensorEventConverter;
    private final HubEventConverter hubEventConverter;
    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorEventHandlers;

    @Value("${kafka.topic.sensors}")
    private String sensorsTopic;

    @Value("${kafka.topic.hubs}")
    private String hubsTopic;

    public CollectorService(KafkaProducer<String, Object> kafkaProducer,
                            SensorEventConverter sensorEventConverter,
                            HubEventConverter hubEventConverter,
                            Set<SensorEventHandler> sensorEventHandlers) {
        this.kafkaProducer = kafkaProducer;
        this.sensorEventConverter = sensorEventConverter;
        this.hubEventConverter = hubEventConverter;
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(
                        SensorEventHandler::getMessageType,
                        Function.identity()
                ));
    }

    public void processSensorEvent(SensorEventProto event) {
        // Находим и вызываем соответствующий обработчик
        SensorEventHandler handler = sensorEventHandlers.get(event.getPayloadCase());
        if (handler != null) {
            try {
                handler.handle(event);
            } catch (Exception e) {
                throw new EventProcessingException(
                        String.format("Failed to handle sensor event: id=%s, type=%s",
                                event.getId(), event.getPayloadCase()), e);
            }
        } else {
            throw new EventProcessingException(
                    String.format("No handler found for sensor event type: %s", event.getPayloadCase()));
        }

        SensorEventAvro avroEvent;
        try {
            avroEvent = sensorEventConverter.convert(event);
        } catch (Exception e) {
            throw new EventConversionException(
                    String.format("Failed to convert sensor event to Avro: id=%s", event.getId()), e);
        }

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                sensorsTopic,
                event.getHubId(),
                avroEvent
        );

        try {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.debug("Sensor event sent to Kafka: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to send sensor event to Kafka: id={}, hubId={}",
                            event.getId(), event.getHubId(), exception);
                    throw new KafkaSendException(
                            String.format("Failed to send sensor event to Kafka: id=%s", event.getId()), exception);
                }
            });
        } catch (Exception e) {
            throw new KafkaSendException(
                    String.format("Error sending sensor event to Kafka: id=%s", event.getId()), e);
        }
    }

    public void processHubEvent(HubEventProto event) {
        log.info("Processing hub event: hubId={}, payloadCase={}",
                event.getHubId(), event.getPayloadCase());

        HubEventAvro avroEvent;
        try {
            avroEvent = hubEventConverter.convert(event);
        } catch (Exception e) {
            throw new EventConversionException(
                    String.format("Failed to convert hub event to Avro: hubId=%s", event.getHubId()), e);
        }

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                hubsTopic,
                event.getHubId(),
                avroEvent
        );

        try {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.debug("Hub event sent to Kafka: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to send hub event to Kafka: hubId={}", event.getHubId(), exception);
                    throw new KafkaSendException(
                            String.format("Failed to send hub event to Kafka: hubId=%s", event.getHubId()), exception);
                }
            });
        } catch (Exception e) {
            throw new KafkaSendException(
                    String.format("Error sending hub event to Kafka: hubId=%s", event.getHubId()), e);
        }
    }

    @PreDestroy
    public void close() {
        log.info("Closing Kafka producer");
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close(Duration.ofSeconds(10));
            log.info("Kafka producer closed");
        }
    }
}