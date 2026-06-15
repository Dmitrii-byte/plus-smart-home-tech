package ru.practirum.collector.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class HubEventConverter {

    public HubEventAvro convert(HubEventProto event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(convertTimestamp(event.getTimestamp()));

        switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEventProto deviceAdded = event.getDeviceAdded();
                builder.setPayload(DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAdded.getId())
                        .setType(convertDeviceType(deviceAdded.getType()))
                        .build());
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEventProto deviceRemoved = event.getDeviceRemoved();
                builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemoved.getId())
                        .build());
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventProto scenarioAdded = event.getScenarioAdded();
                builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAdded.getName())
                        .setConditions(convertConditions(scenarioAdded.getConditionList()))
                        .setActions(convertActions(scenarioAdded.getActionList()))
                        .build());
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEventProto scenarioRemoved = event.getScenarioRemoved();
                builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build());
            }
            default -> {
                log.error("Unknown hub event payload case: {}", event.getPayloadCase());
                throw new IllegalArgumentException("Unknown hub event type: " + event.getPayloadCase());
            }
        }

        return builder.build();
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private DeviceTypeAvro convertDeviceType(DeviceTypeProto deviceType) {
        return switch (deviceType) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Unknown device type: " + deviceType);
        };
    }

    private List<ScenarioConditionAvro> convertConditions(List<ScenarioConditionProto> conditions) {
        List<ScenarioConditionAvro> result = new ArrayList<>();

        for (ScenarioConditionProto condition : conditions) {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId(condition.getSensorId())
                    .setType(convertConditionType(condition.getType()))
                    .setOperation(convertOperation(condition.getOperation()));

            // Устанавливаем значение в зависимости от типа
            switch (condition.getValueCase()) {
                case BOOL_VALUE -> builder.setValue(condition.getBoolValue());
                case INT_VALUE -> builder.setValue(condition.getIntValue());
                case VALUE_NOT_SET -> builder.setValue(null);
            }

            result.add(builder.build());
        }

        return result;
    }

    private List<DeviceActionAvro> convertActions(List<DeviceActionProto> actions) {
        List<DeviceActionAvro> result = new ArrayList<>();

        for (DeviceActionProto action : actions) {
            DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                    .setSensorId(action.getSensorId())
                    .setType(convertActionType(action.getType()));

            // Устанавливаем значение, если оно есть
            if (action.hasValue()) {
                builder.setValue(action.getValue());
            } else {
                builder.setValue(null);
            }

            result.add(builder.build());
        }

        return result;
    }

    private ConditionTypeAvro convertConditionType(ConditionTypeProto type) {
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unknown condition type: " + type);
        };
    }

    private ConditionOperationAvro convertOperation(ConditionOperationProto operation) {
        return switch (operation) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        };
    }

    private ActionTypeAvro convertActionType(ActionTypeProto type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unknown action type: " + type);
        };
    }
}