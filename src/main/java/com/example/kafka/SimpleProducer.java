package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {
        private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

        public static void main(String[] args) {

                final String applicationID = "SimpleProducer";
                final String bootstrapServers = "localhost:9092,localhost:9093";
                final String topicName = "simple";
                final int numMessages = 10;

                logger.info("Creating Kafka Producer...");
                Properties props = new Properties();
                // CLIENT_ID_CONFIG - The purpose of this is to be able to track the source of
                // requests beyond just ip/port by allowing a logical application name to be
                // included in server-side
                // request logging.
                props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationID);
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());

                KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                logger.info("Start sending messages...");
                for (int i = 1; i <= numMessages; i++) {
                        // send message - asynchronous
                        producer.send(new ProducerRecord<>(topicName, "Simple Message-" + i));
                }

                logger.info("Finished - Closing Kafka Producer.");
                producer.close();

        }
}
