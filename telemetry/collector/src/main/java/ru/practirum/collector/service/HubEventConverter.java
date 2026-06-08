package ru.practirum.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.practirum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class HubEventConverter {

    public HubEventAvro convert(HubEvent event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        switch (event.getType()) {
            case DEVICE_ADDED:
                DeviceAddedEvent deviceAdded = (DeviceAddedEvent) event;
                builder.setPayload(DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAdded.getDeviceId())
                        .setType(convertDeviceType(deviceAdded.getDeviceType()))
                        .build());
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEvent deviceRemoved = (DeviceRemovedEvent) event;
                builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemoved.getDeviceId())
                        .build());
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;
                builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAdded.getName())
                        .setConditions(convertConditions(scenarioAdded.getConditions()))
                        .setActions(convertActions(scenarioAdded.getActions()))
                        .build());
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
                builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build());
                break;

            default:
                log.error("Unknown hub event type: {}", event.getType());
                throw new IllegalArgumentException("Unknown hub event type: " + event.getType());
        }

        return builder.build();
    }

    private DeviceTypeAvro convertDeviceType(DeviceType deviceType) {
        switch (deviceType) {
            case MOTION_SENSOR:
                return DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR:
                return DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR:
                return DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR:
                return DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR:
                return DeviceTypeAvro.SWITCH_SENSOR;
            default:
                throw new IllegalArgumentException("Unknown device type: " + deviceType);
        }
    }

    private List<ScenarioConditionAvro> convertConditions(List<Map<String, Object>> conditions) {
        List<ScenarioConditionAvro> result = new ArrayList<>();

        for (Map<String, Object> condition : conditions) {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId((String) condition.get("sensorId"))
                    .setType(convertConditionType((String) condition.get("type")))
                    .setOperation(convertOperation((String) condition.get("operation")));

            // Устанавливаем значение (может быть Integer, Boolean или null)
            Object value = condition.get("value");
            if (value instanceof Integer) {
                builder.setValue((Integer) value);
            } else if (value instanceof Boolean) {
                builder.setValue((Boolean) value);
            } else {
                builder.setValue(null);
            }

            result.add(builder.build());
        }

        return result;
    }

    private List<DeviceActionAvro> convertActions(List<Map<String, Object>> actions) {
        List<DeviceActionAvro> result = new ArrayList<>();

        for (Map<String, Object> action : actions) {
            DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                    .setSensorId((String) action.get("sensorId"))
                    .setType(convertActionType((String) action.get("type")));

            // Устанавливаем значение (может быть Integer или null)
            Object value = action.get("value");
            if (value instanceof Integer) {
                builder.setValue((Integer) value);
            } else {
                builder.setValue(null);
            }

            result.add(builder.build());
        }

        return result;
    }

    private ConditionTypeAvro convertConditionType(String type) {
        switch (type) {
            case "MOTION":
                return ConditionTypeAvro.MOTION;
            case "LUMINOSITY":
                return ConditionTypeAvro.LUMINOSITY;
            case "SWITCH":
                return ConditionTypeAvro.SWITCH;
            case "TEMPERATURE":
                return ConditionTypeAvro.TEMPERATURE;
            case "CO2LEVEL":
                return ConditionTypeAvro.CO2LEVEL;
            case "HUMIDITY":
                return ConditionTypeAvro.HUMIDITY;
            default:
                throw new IllegalArgumentException("Unknown condition type: " + type);
        }
    }

    private ConditionOperationAvro convertOperation(String operation) {
        switch (operation) {
            case "EQUALS":
                return ConditionOperationAvro.EQUALS;
            case "GREATER_THAN":
                return ConditionOperationAvro.GREATER_THAN;
            case "LOWER_THAN":
                return ConditionOperationAvro.LOWER_THAN;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private ActionTypeAvro convertActionType(String type) {
        switch (type) {
            case "ACTIVATE":
                return ActionTypeAvro.ACTIVATE;
            case "DEACTIVATE":
                return ActionTypeAvro.DEACTIVATE;
            case "INVERSE":
                return ActionTypeAvro.INVERSE;
            case "SET_VALUE":
                return ActionTypeAvro.SET_VALUE;
            default:
                throw new IllegalArgumentException("Unknown action type: " + type);
        }
    }
}