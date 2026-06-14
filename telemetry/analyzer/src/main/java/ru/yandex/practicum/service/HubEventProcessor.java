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
import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.Sensor;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final Consumer<String, SpecificRecordBase> hubConsumer;
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Value("${analyzer.kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void run() {
        try {
            hubConsumer.subscribe(List.of(hubsTopic));
            log.info("HubEventProcessor subscribed to topic: {}", hubsTopic);

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, SpecificRecordBase> records =
                        hubConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    if (record.value() instanceof HubEventAvro hubEvent) {
                        processHubEvent(hubEvent);
                    }
                }
                hubConsumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor received shutdown signal");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            try {
                hubConsumer.commitSync();
            } finally {
                hubConsumer.close();
                log.info("HubEventProcessor closed");
            }
        }
    }

    private void processHubEvent(HubEventAvro event) {
        log.info("Received hub event: hubId={}", event.getHubId());

        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            handleDeviceAdded(event.getHubId(), deviceAdded);
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            handleDeviceRemoved(event.getHubId(), deviceRemoved);
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            handleScenarioAdded(event.getHubId(), scenarioAdded);
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            handleScenarioRemoved(event.getHubId(), scenarioRemoved);
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro deviceAdded) {
        String sensorId = deviceAdded.getId();
        if (!sensorRepository.existsById(sensorId)) {
            Sensor sensor = Sensor.builder()
                    .id(sensorId)
                    .hubId(hubId)
                    .build();
            sensorRepository.save(sensor);
            log.info("Added new sensor: sensorId={}, hubId={}", sensorId, hubId);
        } else {
            log.debug("Sensor already exists: sensorId={}", sensorId);
        }
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro deviceRemoved) {
        String sensorId = deviceRemoved.getId();
        sensorRepository.findByIdAndHubId(sensorId, hubId)
                .ifPresent(sensor -> {
                    sensorRepository.delete(sensor);
                    log.info("Removed sensor: sensorId={}, hubId={}", sensorId, hubId);
                });
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro scenarioAdded) {
        String scenarioName = scenarioAdded.getName();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    scenarioRepository.flush();
                });

        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(scenarioName)
                .conditions(new ArrayList<>())
                .actions(new ArrayList<>())
                .build();

        for (ScenarioConditionAvro cond : scenarioAdded.getConditions()) {
            Condition condition = Condition.builder()
                    .scenario(scenario)
                    .type(cond.getType().name())
                    .operation(cond.getOperation().name())
                    .value(getConditionValue(cond))
                    .build();
            scenario.getConditions().add(condition);
        }

        for (DeviceActionAvro act : scenarioAdded.getActions()) {
            Action action = Action.builder()
                    .scenario(scenario)
                    .sensorId(act.getSensorId())
                    .type(act.getType().name())
                    .value(act.getValue() != null ? act.getValue() : null)
                    .build();
            scenario.getActions().add(action);
        }

        scenarioRepository.save(scenario);
        log.info("Added scenario: hubId={}, name={}", hubId, scenarioName);
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro scenarioRemoved) {
        String scenarioName = scenarioRemoved.getName();
        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("Removed scenario: hubId={}, name={}", hubId, scenarioName);
                });
    }

    private Integer getConditionValue(ScenarioConditionAvro condition) {
        Object value = condition.getValue();
        if (value instanceof Integer intValue) {
            return intValue;
        } else if (value instanceof Boolean boolValue) {
            return boolValue ? 1 : 0;
        }
        return null;
    }
}