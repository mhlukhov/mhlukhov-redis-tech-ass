package io.mh;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RedisTechAssAppFunctionalTest {

    private JedisPool jedisPool;
    private Jedis jedis;

    @BeforeEach
    void setUp() {
        jedisPool = new JedisPool("localhost", 6379);
        jedis = jedisPool.getResource();
        // Clear any existing data
        jedis.del(RedisTechAssApp.PROCESSED_STREAM_KEY);
        jedis.del(RedisTechAssApp.CONSUMER_IDS_KEY);
    }

    @AfterEach
    void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    void testConsumerGroupInitialization() {
        RedisTechAssApp.initializeConsumerGroup(jedis);
        // Verify that the consumer group was created
        List<StreamGroupInfo> streamGroupInfos = jedis.xinfoGroups(RedisTechAssApp.PROCESSED_STREAM_KEY);
        assertFalse(streamGroupInfos.isEmpty());
    }

    @Test
    void testConsumerListInitialization() {
        int groupSize = 3;
        RedisTechAssApp.initializeConsumerList(jedis, groupSize);
        // Verify that the consumer list was created
        List<String> consumerIds = jedis.lrange(RedisTechAssApp.CONSUMER_IDS_KEY, 0, -1);
        assertEquals(groupSize, consumerIds.size());
    }

    @Test
    void testMessageProcessing() {
        String consumerId = "consumer_0";
        Map<String, String> message = Collections.singletonMap("message", "test message");
        RedisTechAssApp.processMessage(jedis, consumerId, message);
        // Verify that the message was added to the processed stream
        List<StreamEntry> entries = jedis.xrange(RedisTechAssApp.PROCESSED_STREAM_KEY, StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, 1);
        assertFalse(entries.isEmpty());
        Map<String, String> processedMessage = entries.get(0).getFields();
        assertEquals("test message", processedMessage.get("message"));
        assertEquals(consumerId, processedMessage.get("processed_by"));
        assertNotNull(processedMessage.get("processing_time"));
    }
}