package com.contactsunny.poc.SimpleKafkaProducer;

import com.contactsunny.poc.SimpleKafkaProducer.kafkaConsumers.BinlogSimpleKafkaConsumer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class SimpleKafkaConsumerApplication implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topicName;

    @Value("${kafka.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${zookeeper.host}")
    String zookeeperHost;

    private static final Logger logger = Logger.getLogger(SimpleKafkaConsumerApplication.class);

    public static void main( String[] args ) {
        SpringApplication.run(SimpleKafkaConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) {


        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("group.id", zookeeperGroupId);
        consumerProperties.put("auto.commit.interval.ms", "100");
        consumerProperties.put("max.poll.records", "1000");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /*
         * Creating a thread to listen to the kafka topic
         */
        Thread kafkaConsumerThread = new Thread(() -> {
            logger.info("Starting Kafka consumer thread.");

            BinlogSimpleKafkaConsumer binlogSimpleKafkaConsumer = new BinlogSimpleKafkaConsumer(
                    topicName,
                    consumerProperties
            );

            binlogSimpleKafkaConsumer.runSingleWorker();
        });

        /*
         * Starting the first thread.
         */
        kafkaConsumerThread.start();
    }
}
