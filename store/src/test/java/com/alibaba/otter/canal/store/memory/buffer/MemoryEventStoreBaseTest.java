package com.alibaba.otter.canal.store.memory.buffer;

import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.memory.utils.EventMockUtil;
import com.alibaba.otter.canal.store.model.BatchMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MemoryEventStoreBaseTest {

    @Test
    public void testOnePut() {
        MemoryEventStoreWithBuffer eventStore = getMemoryEventStore();
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();
        try {
            eventStore.put(EventMockUtil.buildEvent("1", 1L, 1L, 1024));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // 尝试阻塞+超时
        boolean result = false;
        try {
            result = eventStore.put(EventMockUtil.buildEvent("1", 1L, 1L), 1000L, TimeUnit.MILLISECONDS);
            Assert.assertTrue(result);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // 尝试
        result = eventStore.tryPut(EventMockUtil.buildEvent("1", 1L, 1L));
        Assert.assertTrue(result);

        eventStore.stop();
    }

    public MemoryEventStoreWithBuffer getMemoryEventStore() {
        return new MemoryEventStoreWithBuffer();
    }

}
