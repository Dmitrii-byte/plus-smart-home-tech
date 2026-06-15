package ru.practirum.collector.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@Component
public class ClimateSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var climateSensor = event.getClimateSensor();
        log.info("Processing climate sensor event: id={}, temperatureC={}, humidity={}, co2Level={}",
                event.getId(), climateSensor.getTemperatureC(),
                climateSensor.getHumidity(), climateSensor.getCo2Level());
    }
}