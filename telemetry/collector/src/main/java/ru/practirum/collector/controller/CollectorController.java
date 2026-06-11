package ru.practirum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import ru.practirum.collector.model.hub.HubEvent;
import ru.practirum.collector.model.sensor.SensorEvent;
import ru.practirum.collector.service.CollectorService;

@Slf4j
@Controller
@RequestMapping("/events")
@RequiredArgsConstructor
public class CollectorController {
    private final CollectorService collectorService;

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: type={}, id={}, hubId={}",
                event.getType(), event.getId(), event.getHubId());
        collectorService.processSensorEvent(event);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Received hub event: type={}, hubId={}",
                event.getType(), event.getHubId());
        collectorService.processHubEvent(event);
        return ResponseEntity.ok().build();
    }
}