package io.mh;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

class RedisTechAssAppTest {

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @AfterEach
    void tearDown() {
        jedis.close();
    }

    @Test
    void testInitializeConsumerGroup() {
        RedisTechAssApp.initializeConsumerGroup(jedis);
        verify(jedis, times(1)).xgroupCreate(anyString(), anyString(), any(StreamEntryID.class), eq(true));
    }

    @Test
    void testInitializeConsumerList() {
        int groupSize = 3;
        RedisTechAssApp.initializeConsumerList(jedis, groupSize);
        verify(jedis, times(1)).del(eq(RedisTechAssApp.CONSUMER_IDS_KEY));
        verify(jedis, times(groupSize)).rpush(eq(RedisTechAssApp.CONSUMER_IDS_KEY), anyString());
    }

    @Test
    void testProcessMessage() {
        String consumerId = "consumer_0";
        Map<String, String> message = new HashMap<>();
        message.put("message", "test message");

        RedisTechAssApp.processMessage(jedis, consumerId, message);
        verify(jedis, times(1)).xadd(eq(RedisTechAssApp.PROCESSED_STREAM_KEY), any(XAddParams.class), eq(message));
    }

    @Test
    void testReportMetrics() {
        RedisTechAssApp.setMessagesProcessedInitValue(10);
        RedisTechAssApp.reportMetrics();
    }
}