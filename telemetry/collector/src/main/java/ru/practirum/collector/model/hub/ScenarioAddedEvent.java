package ru.practirum.collector.model.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioAddedEvent extends HubEvent {
    private String name;
    private List<Map<String, Object>> conditions; // упрощенно, как в JSON
    private List<Map<String, Object>> actions;    // упрощенно, как в JSON

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}