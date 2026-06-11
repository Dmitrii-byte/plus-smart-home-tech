package ru.practirum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class DeviceAddedEvent extends HubEvent {
    @JsonProperty("id")
    private String deviceId;
    private DeviceType deviceType; // MOTION_SENSOR, TEMPERATURE_SENSOR и т.д.

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}