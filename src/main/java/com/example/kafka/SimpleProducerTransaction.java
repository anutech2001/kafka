package com.example.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerTransaction {
        private static final Logger logger =
                        LoggerFactory.getLogger(SimpleProducerTransaction.class);

        public static void main(String[] args) {

                final String applicationID = "SimpleProducerTransaction";
                final String bootstrapServers = "localhost:9092,localhost:9093";
                final String topicName1 = "simpleTrx1";
                final String topicName2 = "simpleTrx2";
                final String transactionId = "simple-trx-id";
                final int numMessages = 10;

                logger.info("Creating Kafka Producer...");
                Properties props = new Properties();
                props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationID);
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());

                KafkaProducer<String, String> producer = new KafkaProducer<>(props);
                producer.initTransactions();

                logger.info("Start sending messages...");
                producer.beginTransaction();

                try {
                        for (int i = 1; i <= numMessages; i++) {
                                producer.send(new ProducerRecord<>(topicName1,
                                                "Message to simpleTrx1-" + i));
                                producer.send(new ProducerRecord<>(topicName2,
                                                "Message to simpleTrx2-" + i));
                        }
                        // Thread.sleep(5000l);
                        producer.commitTransaction();
                        producer.close();
                } catch (Exception e) {
                        logger.error("Error in sending messages", e);
                        producer.abortTransaction();
                        producer.close();
                        throw new RuntimeException(e);
                }
        }
}
