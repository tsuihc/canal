package com.alibaba.otter.canal.store.memory;

import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentMemoryEventStoreWithBuffer extends MemoryEventStoreWithBuffer {

    protected AtomicLong puttingSequence = new AtomicLong(0);






}
