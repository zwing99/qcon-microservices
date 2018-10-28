package io.confluent.qcon.orders.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Load configuration from a file. This is mainly intended for connection info, so I can switch between clusters without recompile
 */
public class LoadConfigs {

    private static final Logger log = LoggerFactory.getLogger(LoadConfigs.class);

    // default to cloud, duh
    private static final String DEFAULT_CONFIG_FILE =
            System.getProperty("user.home") + File.separator + ".ccloud" + File.separator + "config";

    public static String parseArgsAndConfigure(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("usage: ... " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_CONFIG_FILE + ")] ");
        }

        final String configFile = args.length > 0 ? args[0] : DEFAULT_CONFIG_FILE;

        log.info("Connecting to Kafka cluster using config file " + configFile);
        return configFile;
    }

    public static Properties loadConfig(String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new RuntimeException(configFile + " does not exist. You need a file with client configuration, " +
                    "either create one or run `ccloud init` if you are a Confluent Cloud user");
        }
        System.out.println("Loading configs from:" + configFile);
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }

        return cfg;
    }
}
