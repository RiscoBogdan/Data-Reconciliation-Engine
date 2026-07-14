package com.reconciler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Spring Boot entry point.
 * All wiring is done via @ConfigurationProperties, @Service, @RestController.
 */
@SpringBootApplication
@ConfigurationPropertiesScan("com.reconciler.config")
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
