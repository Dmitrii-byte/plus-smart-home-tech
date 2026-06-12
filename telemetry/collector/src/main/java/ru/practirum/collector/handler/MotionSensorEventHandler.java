package ru.practirum.collector.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@Component
public class MotionSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var motionSensor = event.getMotionSensor();
        log.info("Processing motion sensor event: id={}, motion={}, linkQuality={}, voltage={}",
                event.getId(), motionSensor.getMotion(),
                motionSensor.getLinkQuality(), motionSensor.getVoltage());
    }
}