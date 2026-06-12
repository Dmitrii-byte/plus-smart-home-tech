package ru.practirum.collector.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@Component
public class LightSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var lightSensor = event.getLightSensor();
        log.info("Processing light sensor event: id={}, luminosity={}, linkQuality={}",
                event.getId(), lightSensor.getLuminosity(), lightSensor.getLinkQuality());
    }
}