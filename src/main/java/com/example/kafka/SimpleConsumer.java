package com.example.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
        private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

        public static void main(String[] args) {
                final String applicationID = "SimpleConsumer";
                final String bootstrapServers = "localhost:9092,localhost:9093";
                final String groupID = "SimpleConsumer";
                final String[] sourceTopicNames = {"simple", "A"};

                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationID);
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                consumer.subscribe(Arrays.asList(sourceTopicNames));

                while (true) {

                        ConsumerRecords<String, String> records =
                                        consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> record : records) {
                                logger.info("Message - " + record.value() + ", Topic: "
                                                + record.topic() + ", Partition No: "
                                                + record.partition() + ", Offset No: "
                                                + record.offset());
                                if (record.headers().toArray().length > 0) {
                                        record.headers().forEach(h -> {
                                                logger.info("    Header Key: " + h.key()
                                                                + ", Value: "
                                                                + new String(h.value()));
                                        });

                                }
                        }

                }
        }
}
