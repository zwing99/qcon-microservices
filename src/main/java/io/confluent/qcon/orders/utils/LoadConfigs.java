package io.confluent.qcon.orders.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
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

    public static Properties configStreams(String configFile, String stateDir, String serviceID) throws IOException {
        Properties props = LoadConfigs.loadConfig(configFile);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, serviceID);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Recommended cloud configuration for Streams (basically, wait for longer before exiting if brokers disconnect)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
        props.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);

        return props;
    }
}
