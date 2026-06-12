package ru.practirum.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.practirum.aggregator.service.AggregationStarter;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Aggregator {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);

        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        aggregator.start();
    }
}