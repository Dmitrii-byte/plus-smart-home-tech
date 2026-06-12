package ru.practirum.collector.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Slf4j
@Component
public class SensorEventConverter {

    public SensorEventAvro convert(SensorEventProto event) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(convertTimestamp(event.getTimestamp()));

        switch (event.getPayloadCase()) {
            case MOTION_SENSOR -> {
                MotionSensorProto motionSensor = event.getMotionSensor();
                builder.setPayload(MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensor.getLinkQuality())
                        .setMotion(motionSensor.getMotion())
                        .setVoltage(motionSensor.getVoltage())
                        .build());
            }
            case TEMPERATURE_SENSOR -> {
                TemperatureSensorProto tempSensor = event.getTemperatureSensor();
                builder.setPayload(TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(tempSensor.getTemperatureC())
                        .setTemperatureF(tempSensor.getTemperatureF())
                        .build());
            }
            case LIGHT_SENSOR -> {
                LightSensorProto lightSensor = event.getLightSensor();
                builder.setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensor.getLinkQuality())
                        .setLuminosity(lightSensor.getLuminosity())
                        .build());
            }
            case CLIMATE_SENSOR -> {
                ClimateSensorProto climateSensor = event.getClimateSensor();
                builder.setPayload(ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSensor.getTemperatureC())
                        .setHumidity(climateSensor.getHumidity())
                        .setCo2Level(climateSensor.getCo2Level())
                        .build());
            }
            case SWITCH_SENSOR -> {
                SwitchSensorProto switchSensor = event.getSwitchSensor();
                builder.setPayload(SwitchSensorAvro.newBuilder()
                        .setState(switchSensor.getState())
                        .build());
            }
            default -> {
                log.error("Unknown sensor event payload case: {}", event.getPayloadCase());
                throw new IllegalArgumentException("Unknown sensor event type: " + event.getPayloadCase());
            }
        }

        return builder.build();
    }

    private Instant convertTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}