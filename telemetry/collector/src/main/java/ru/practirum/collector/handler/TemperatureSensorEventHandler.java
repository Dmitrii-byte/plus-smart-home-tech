package ru.practirum.collector.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var temperatureSensor = event.getTemperatureSensor();
        log.info("Processing temperature sensor event: id={}, temperatureC={}, temperatureF={}",
                event.getId(), temperatureSensor.getTemperatureC(),
                temperatureSensor.getTemperatureF());
    }
}