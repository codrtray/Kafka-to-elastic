package com.dmi.kafkatoelastic;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class MainConsumer {
    public static void main(String[] args) {

        if (args.length != 1) {
            throw new IllegalArgumentException("It need to pass argument");
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ElasticSearchConsumer searchConsumer = new ElasticSearchConsumer(args[0], countDownLatch);

        new Thread(searchConsumer).start();

        Runtime.getRuntime().addShutdownHook(getHook(countDownLatch, searchConsumer));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Thread getHook(CountDownLatch countDownLatch, ElasticSearchConsumer searchConsumer) {
        return new Thread(() -> {
            log.info("Caught shutdown hook");
            searchConsumer.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has closed");
        });
    }
}
