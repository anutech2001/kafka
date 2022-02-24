package com.example.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class ConsumerProducerTransaction {
        private static final Logger logger =
                        LoggerFactory.getLogger(ConsumerProducerTransaction.class);

        public static void main(String[] args) {

                final String applicationID = "ConsumerProducerTransactionApp";
                final String bootstrapServers = "localhost:9092,localhost:9093";
                final String groupID = "InputConsumerGrp";
                final String[] sourceTopicNames = {"input"};
                final String outputTopic = "output";
                final String transactionId = "consumerproducer-trx-id";

                logger.info("Creating Kafka Consumer...");
                Properties consumerProps = new Properties();
                // consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationID);
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                logger.info("Creating Kafka Producer...");
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, applicationID);
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                KafkaJsonSerializer.class.getName());
                producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                                "com.example.kafka.CountingProducerInterceptor");

                KafkaProducer<String, TransactionEvent> producer =
                                new KafkaProducer<String, TransactionEvent>(producerProps);
                producer.initTransactions();

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                consumer.subscribe(Arrays.asList(sourceTopicNames));

                while (true) {
                        try {
                                ConsumerRecords<String, String> records =
                                                consumer.poll(Duration.ofMillis(200));
                                if (records.count() > 0) {
                                        producer.beginTransaction();
                                        for (ConsumerRecord<String, String> record : records) {
                                                logger.info("Received message: " + record.value());
                                                ProducerRecord<String, TransactionEvent> producerRecord =
                                                                new ProducerRecord<>(outputTopic,
                                                                                createTransactionEvent(
                                                                                                record.value()));
                                                logger.info("Sending message: " + producerRecord
                                                                .value().toString());
                                                producer.send(producerRecord);
                                        }

                                        // Commit the offset of the last message
                                        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                                                        new HashMap<>();
                                        for (TopicPartition partition : records.partitions()) {
                                                List<ConsumerRecord<String, String>> partitionedRecords =
                                                                records.records(partition);
                                                long offset = partitionedRecords
                                                                .get(partitionedRecords.size() - 1)
                                                                .offset();
                                                offsetsToCommit.put(partition,
                                                                new OffsetAndMetadata(offset + 1));
                                        }
                                        producer.sendOffsetsToTransaction(offsetsToCommit, groupID);
                                        // Commit all transactions
                                        producer.commitTransaction();
                                        logger.info("Commit transactions completed....");
                                }
                        } catch (Exception e) {
                                logger.error("Error while doing transactions.", e);
                                logger.info("Aborting transactions...");
                                producer.abortTransaction();
                                resetToLastCommittedPositions(consumer);
                        }
                }

        }

        private static TransactionEvent createTransactionEvent(String value) {
                // format trxCode:trxName:trxAmount
                logger.info("Creating TransactionEvent from value: " + value);
                String[] trxDetails = value.split("\\|");
                TransactionEvent event =
                                new TransactionEvent(trxDetails[0], trxDetails[1], trxDetails[2]);
                return event;
        }

        private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
                final Map<TopicPartition, OffsetAndMetadata> committed =
                                consumer.committed(consumer.assignment());
                consumer.assignment().forEach(tp -> {
                        OffsetAndMetadata offsetAndMetadata = committed.get(tp);
                        if (offsetAndMetadata != null)
                                consumer.seek(tp, offsetAndMetadata.offset());
                        else
                                consumer.seekToBeginning(Collections.singleton(tp));
                });
        }
}
