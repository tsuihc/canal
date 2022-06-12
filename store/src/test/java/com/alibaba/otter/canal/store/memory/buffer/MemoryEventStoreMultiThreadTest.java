package com.alibaba.otter.canal.store.memory.buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 多线程的put/get/ack/rollback测试
 *
 * @author jianghang 2012-6-20 下午02:50:36
 * @version 1.0.0
 */
@SuppressWarnings("BusyWait")
public class MemoryEventStoreMultiThreadTest extends MemoryEventStoreBase {

    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    private MemoryEventStoreWithBuffer eventStore;

    @Before
    public void setUp() {
        eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(16 * 16);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();
    }

    @After
    public void tearDown() {
        eventStore.stop();
    }

    @Ignore
    @Test
    public void test() {
        CountDownLatch latch = new CountDownLatch(1);
        BooleanMutex mutex = new BooleanMutex(true);
        Producer producer = new Producer(mutex, 10);
        Consumer consumer = new Consumer(latch, 20, 50);

        executor.submit(producer);
        executor.submit(consumer);

        try {
            Thread.sleep(30 * 1000L);
        } catch (InterruptedException ignored) {
        }

        mutex.set(false);
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
        executor.shutdown();

        List<Long> result = consumer.getResult();

        long last = -1L;
        for (Long offset : result) {
            Assert.assertEquals(last + 1, (long) offset);// 取出来的数据一定是递增的
            last = offset;
        }
    }

    class Producer implements Runnable {

        private final BooleanMutex mutex;
        private final int freq;

        public Producer(BooleanMutex mutex, int freq) {
            this.mutex = mutex;
            this.freq = freq;
        }

        public void run() {
            long offset = 0;
            while (true) {
                try {
                    mutex.get();
                    Thread.sleep(RandomUtils.nextInt(freq));
                } catch (InterruptedException e) {
                    return;
                }
                Event event = buildEvent("1", offset++, 1L);

                try {
                    Thread.sleep(RandomUtils.nextInt(freq));
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    eventStore.put(event);
                } catch (CanalStoreException | InterruptedException ignored) {
                }
            }
        }
    }

    class Consumer implements Runnable {

        private final CountDownLatch latch;
        private final int freq;
        private final int batchSize;
        private final List<Long> result = new ArrayList<>();

        public Consumer(CountDownLatch latch, int freq, int batchSize) {
            this.latch = latch;
            this.freq = freq;
            this.batchSize = batchSize;
        }

        public void run() {
            Position first = eventStore.getFirstPosition();
            while (first == null) {
                try {
                    Thread.sleep(RandomUtils.nextInt(freq));
                } catch (InterruptedException e) {
                    latch.countDown();
                    return;
                }

                first = eventStore.getFirstPosition();
            }

            int ackCount = 0;
            int emptyCount = 0;
            while (emptyCount < 10) {
                try {
                    Thread.sleep(RandomUtils.nextInt(freq));
                } catch (InterruptedException ignored) {
                }

                try {
                    Events<Event> entries = eventStore.get(first, batchSize, 1000L, TimeUnit.MILLISECONDS);

                    if (!CollectionUtils.isEmpty(entries.getEvents())) {
                        if (entries.getEvents().size() != batchSize) {
                            System.out.println("get size:" + entries.getEvents().size() + " with not full batchSize:" + batchSize);
                        }

                        first = entries.getPositionRange().getEnd();
                        for (Event event : entries.getEvents()) {
                            this.result.add(event.getPosition());
                        }
                        emptyCount = 0;

                        System.out.println("offset : " + entries.getEvents().get(0).getPosition() + " , count :" + entries.getEvents().size());
                        ackCount++;
                        if (ackCount == 1) {
                            eventStore.cleanUntil(entries.getPositionRange().getEnd());
                            System.out.println("first position : " + eventStore.getFirstPosition());
                            ackCount = 0;
                        }
                    } else {
                        emptyCount++;
                        System.out.println("empty events for " + emptyCount);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            latch.countDown();
        }

        public List<Long> getResult() {
            return result;
        }

    }
}
