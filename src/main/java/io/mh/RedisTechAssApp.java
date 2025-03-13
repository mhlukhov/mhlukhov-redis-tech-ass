package io.mh;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RedisTechAssApp is a Java application that utilizes Redis Streams and Pub/Sub
 * for message processing. It initializes consumer groups, consumes messages,
 * processes them, and reports metrics periodically.
 */
public class RedisTechAssApp {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final String PUBSUB_CHANNEL = "messages:published";
    protected static final String CONSUMER_IDS_KEY = "consumer:ids";
    protected static final String PROCESSED_STREAM_KEY = "messages:processed";
    private static final String GROUP_NAME = "message_consumers";
    private static final String CONSUMER_ID_PREFIX = "consumer_";

    private static final AtomicInteger messagesProcessed = new AtomicInteger(0);
    private static final long startTime = System.currentTimeMillis();

    /**
     * The entry point of the application.
     *
     * @param args Command-line arguments. Requires the group size as the first argument.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please provide a group size");
            System.out.println("Usage: java RedisTechAssApp <group_size>");
            return;
        }

        int groupSize = Integer.parseInt(args[0]);
        try (JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT)) {
            // Initialize the consumer group
            try (Jedis jedis = jedisPool.getResource()) {
                initializeConsumerGroup(jedis);
                initializeConsumerList(jedis, groupSize);
            }

            // Start the metrics reporter
            try (ScheduledExecutorService metricsScheduler = Executors.newScheduledThreadPool(1)) {
                metricsScheduler.scheduleAtFixedRate(RedisTechAssApp::reportMetrics, 3, 3, TimeUnit.SECONDS);
            }

            // Create a thread pool for consumers
            try (ExecutorService consumerExecutor = Executors.newFixedThreadPool(groupSize)) {
                // Start consumers
                for (int i = 0; i < groupSize; i++) {
                    String consumerId = CONSUMER_ID_PREFIX + i;
                    consumerExecutor.submit(() -> consumeMessages(jedisPool, consumerId));
                }
            }
        }
    }

    /**
     * Initializes the Redis consumer group.
     *
     * @param jedis The Redis client instance.
     */
    protected static void initializeConsumerGroup(Jedis jedis) {
        try {
            jedis.xgroupCreate(PROCESSED_STREAM_KEY, GROUP_NAME, StreamEntryID.NEW_ENTRY, true);
        } catch (Exception e) {
            System.out.println("Consumer group already exists or other error: " + e.getMessage());
        }
    }

    /**
     * Initializes the consumer list in Redis.
     *
     * @param jedis     The Redis client instance.
     * @param groupSize The number of consumers to initialize.
     */
    protected static void initializeConsumerList(Jedis jedis, int groupSize) {
        jedis.del(CONSUMER_IDS_KEY);
        for (int i = 0; i < groupSize; i++) {
            jedis.rpush(CONSUMER_IDS_KEY, CONSUMER_ID_PREFIX + i);
        }
    }

    /**
     * Subscribes a consumer to the Redis Pub/Sub channel to process messages.
     *
     * @param jedisPool  The Redis connection pool.
     * @param consumerId The unique identifier for the consumer.
     */
    protected static void consumeMessages(JedisPool jedisPool, String consumerId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(new RedisPubSubListener(jedisPool, consumerId), PUBSUB_CHANNEL);
        }
    }

    /**
     * Processes a received message and stores it in the Redis stream.
     *
     * @param jedis      The Redis client instance.
     * @param consumerId The identifier of the consumer processing the message.
     * @param message    The message data as a key-value map.
     */
    protected static void processMessage(Jedis jedis, String consumerId, Map<String, String> message) {
        message.put("processed_by", consumerId);
        message.put("processing_time", Instant.now().toString());
        jedis.xadd(PROCESSED_STREAM_KEY, XAddParams.xAddParams(), message);
        messagesProcessed.incrementAndGet();
    }

    /**
     * Reports message processing metrics at a fixed interval.
     */
    protected static void reportMetrics() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = (currentTime - startTime) / 1000;
        if (elapsedTime > 0) {
            double messagesPerSecond = (double) messagesProcessed.get() / elapsedTime;
            System.out.printf("Messages processed per second: %.2f%n", messagesPerSecond);
        }
    }

    /**
     * Redis Pub/Sub Listener to handle incoming messages and process them.
     */
    private static class RedisPubSubListener extends redis.clients.jedis.JedisPubSub {
        private final JedisPool jedisPool;
        private final String consumerId;

        /**
         * Constructs a new RedisPubSubListener.
         *
         * @param jedisPool  The Redis connection pool.
         * @param consumerId The consumer's unique identifier.
         */
        public RedisPubSubListener(JedisPool jedisPool, String consumerId) {
            this.jedisPool = jedisPool;
            this.consumerId = consumerId;
        }

        @Override
        public void onMessage(String channel, String message) {
            if (PUBSUB_CHANNEL.equals(channel)) {
                try (Jedis jedis = jedisPool.getResource()) {
                    // Process the message
                    Map<String, String> messageMap = Collections.singletonMap("message", message);
                    processMessage(jedis, consumerId, messageMap);
                }
            }
        }
    }

    /**
     * Resets the message counter (for testing purposes).
     *
     * @param value The new value to set for the counter.
     */
    public static void setMessagesProcessedInitValue(int value) {
        messagesProcessed.set(value);
    }
}
