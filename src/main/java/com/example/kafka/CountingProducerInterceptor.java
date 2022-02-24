package com.example.kafka;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/*
 * Print number of messages sent and messages acknowledged every N milliseconds
 */
public class CountingProducerInterceptor implements ProducerInterceptor {

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    public ProducerRecord onSend(ProducerRecord producerRecord) {
        numSent.incrementAndGet();
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        numAcked.incrementAndGet();
    }

    public void close() {
        executorService.shutdownNow();
    }

    public void configure(Map<String, ?> map) {
        Long windowSize = 10000l;
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize,
                windowSize, TimeUnit.MILLISECONDS);
    }

    public static void run() {
        System.out.println("Number Sent: " + numSent.getAndSet(0));
        System.out.println("Number Acked: " + numAcked.getAndSet(0));
    }
}
