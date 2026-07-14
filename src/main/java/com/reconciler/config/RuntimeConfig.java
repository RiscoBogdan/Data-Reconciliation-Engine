package com.reconciler.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lightweight runtime config loader that mimics Spring-style application.yml
 * resolution without introducing Spring Boot.
 *
 * <p>Precedence (highest to lowest):
 * <ol>
 *   <li>Environment variables (existing ORACLE_* and SNOWFLAKE_* keys)</li>
 *   <li>application.yml / application.yaml (external or classpath)</li>
 *   <li>Hard-coded default (if provided by caller)</li>
 * </ol>
 */
public final class RuntimeConfig {

    private static final Logger log = LoggerFactory.getLogger(RuntimeConfig.class);

    private static volatile Map<String, Object> yamlConfig;

    private RuntimeConfig() {}

    /** Resolve a required value using env first, then YAML path. */
    public static String require(String envKey, String... yamlPath) {
        String value = optional(envKey, null, yamlPath);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Required configuration missing. Provide env var " + envKey
                            + " or YAML path " + String.join(".", yamlPath));
        }
        return value;
    }

    /** Resolve an optional value with a default fallback. */
    public static String optional(String envKey, String defaultValue, String... yamlPath) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }

        String yamlVal = getYamlValue(yamlPath);
        if (yamlVal != null && !yamlVal.isBlank()) {
            return yamlVal;
        }

        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    private static String getYamlValue(String... path) {
        Map<String, Object> cfg = yaml();
        Object current = cfg;
        for (String segment : path) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = ((Map<String, Object>) map).get(segment);
            if (current == null) {
                return null;
            }
        }
        return current.toString();
    }

    private static Map<String, Object> yaml() {
        Map<String, Object> local = yamlConfig;
        if (local != null) return local;

        synchronized (RuntimeConfig.class) {
            if (yamlConfig == null) {
                yamlConfig = loadYaml();
            }
            return yamlConfig;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadYaml() {
        Path fromProperty = null;
        String configFile = System.getenv("APP_CONFIG_FILE");
        if (configFile != null && !configFile.isBlank()) {
            fromProperty = Path.of(configFile);
        }

        var candidates = new LinkedHashMap<String, InputStream>();

        try {
            if (fromProperty != null && Files.exists(fromProperty)) {
                candidates.put(fromProperty.toAbsolutePath().toString(), Files.newInputStream(fromProperty));
            }

            Path yml = Path.of("application.yml");
            Path yaml = Path.of("application.yaml");
            if (Files.exists(yml)) candidates.put(yml.toAbsolutePath().toString(), Files.newInputStream(yml));
            if (Files.exists(yaml)) candidates.put(yaml.toAbsolutePath().toString(), Files.newInputStream(yaml));

            InputStream classpathYml = RuntimeConfig.class.getClassLoader().getResourceAsStream("application.yml");
            if (classpathYml != null) candidates.put("classpath:application.yml", classpathYml);

            InputStream classpathYaml = RuntimeConfig.class.getClassLoader().getResourceAsStream("application.yaml");
            if (classpathYaml != null) candidates.put("classpath:application.yaml", classpathYaml);

            for (var entry : candidates.entrySet()) {
                try (InputStream in = entry.getValue()) {
                    Object parsed = new Yaml().load(in);
                    if (parsed instanceof Map<?, ?> map) {
                        log.info("Loaded configuration from {}", entry.getKey());
                        return (Map<String, Object>) map;
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Could not read application.yml: {}", e.getMessage());
        }

        log.info("No application.yml found. Using env vars only.");
        return Collections.emptyMap();
    }
}

