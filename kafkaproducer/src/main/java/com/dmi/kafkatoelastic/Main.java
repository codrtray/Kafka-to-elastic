package com.dmi.kafkatoelastic;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Main {

    private final ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();
    private final Producer producer;

    public Main(Producer producer) {
        this.producer = producer;
    }

    public static void main(String[] args) {

        if (args.length != 1) {
            log.error("It needs to pass topic name");
            return;
        }

        log.info("Topic name is {}", args[0]);

        Producer producer = new Producer(args[0]);
        Main main = new Main(producer);

        Runtime.getRuntime()
                .addShutdownHook(new Thread((main.getShutdownTask())));

        main.start();
    }

    public void start() {
        schedule.scheduleAtFixedRate(producer, 0, 1, TimeUnit.SECONDS);
    }

    public Runnable getShutdownTask() {
        return () -> {
            log.info("closing producer ...");
            schedule.shutdown();
            try {
                if (!schedule.awaitTermination(10, TimeUnit.SECONDS)) {
                    schedule.shutdownNow();
                }
            } catch (InterruptedException e) {
                schedule.shutdownNow();
            }
            producer.closeProducer();
            log.info("Producer is closed.");
        };
    }


}
