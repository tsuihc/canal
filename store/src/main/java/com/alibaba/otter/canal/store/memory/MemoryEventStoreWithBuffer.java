package com.alibaba.otter.canal.store.memory;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {

    protected static final long INIT_SEQUENCE = -1;
    protected int bufferSize = 16 * 1024;
    protected long bufferMemUnit = 1024L;                                         // memorySize的单位，默认为1kb大小
    protected int indexMask;
    protected Event[] buffer;

    // 记录下put/get/ack操作的三个下标
    protected AtomicLong putSequence = new AtomicLong(INIT_SEQUENCE);             // 代表当前put操作最后一次写操作发生的位置
    protected AtomicLong getSequence = new AtomicLong(INIT_SEQUENCE);             // 代表当前get操作读取的最后一条的位置
    protected AtomicLong ackSequence = new AtomicLong(INIT_SEQUENCE);             // 代表当前ack操作的最后一条的位置

    // 记录下put/get/ack操作的三个memorySize大小
    protected AtomicLong putMemorySize = new AtomicLong(0);
    protected AtomicLong getMemorySize = new AtomicLong(0);
    protected AtomicLong ackMemorySize = new AtomicLong(0);

    // 记录下put/get/ack操作的三个execTime
    protected AtomicLong putExecTime = new AtomicLong(System.currentTimeMillis());
    protected AtomicLong getExecTime = new AtomicLong(System.currentTimeMillis());
    protected AtomicLong ackExecTime = new AtomicLong(System.currentTimeMillis());

    // 记录下put/get/ack操作的三个table rows
    protected AtomicLong putTableRows = new AtomicLong(0);
    protected AtomicLong getTableRows = new AtomicLong(0);
    protected AtomicLong ackTableRows = new AtomicLong(0);

    protected BatchMode batchMode = BatchMode.ITEMSIZE;                        // 默认为内存大小模式
    protected boolean ddlIsolation = false;
    protected boolean raw = true;                                              // 针对entry是否开启raw模式

    public MemoryEventStoreWithBuffer() {
    }

    public MemoryEventStoreWithBuffer(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    @Override
    public void start() {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("The size of buffer must be a power of 2");
        }
        indexMask = bufferSize - 1;
        buffer = new Event[bufferSize];
    }

    @Override
    public void stop() {
        super.stop();
        cleanAll();
    }

    @Override
    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }
        do {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } while (!checkFreeSlotAt(putSequence.get() + data.size()));
        doPut(data);
    }

    @Override
    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }
        long nanos = unit.toNanos(timeout);
        for (; ; ) {
            long startNanos = System.nanoTime();
            if (checkFreeSlotAt(putSequence.get() + data.size())) {
                doPut(data);
                return true;
            }
            nanos -= (System.nanoTime() - startNanos);
            if (nanos <= 0) {
                return false;
            }
        }
    }

    @Override
    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }
        if (!checkFreeSlotAt(putSequence.get() + data.size())) {
            return false;
        } else {
            doPut(data);
            return true;
        }
    }

    @Override
    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Collections.singletonList(data));
    }

    @Override
    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Collections.singletonList(data), timeout, unit);
    }

    @Override
    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Collections.singletonList(data));
    }

    protected void doPut(List<Event> data) {
        long current = putSequence.get();
        long end = current + data.size();

        // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ring buffer中的老的Entry值
        for (long next = current + 1; next <= end; next++) {
            buffer[getIndex(next)] = data.get((int) (next - current - 1));
        }
        putSequence.set(end);
        // 记录一下gets memorySize信息，方便快速检索
        if (batchMode.isMemSize()) {
            long size = 0;
            for (Event event : data) {
                size += calculateSize(event);
            }
            putMemorySize.getAndAdd(size);
        }
        profiling(data, OP.PUT);
    }

    @Override
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        do {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } while (!checkUnGetSlotAt((LogPosition) start, batchSize));
        return doGet(start, batchSize);
    }

    @Override
    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        long nanos = unit.toNanos(timeout);
        for (; ; ) {
            long startNanos = System.nanoTime();
            if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                return doGet(start, batchSize);
            }
            nanos -= (System.nanoTime() - startNanos);
            if (nanos <= 0) {
                // 如果时间到了，有多少取多少
                return doGet(start, batchSize);
            }
        }
    }

    @Override
    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        return doGet(start, batchSize);
    }

    protected Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        long current = getSequence.get();
        long maxAbleSequence = putSequence.get();
        long next = current;
        long end = current;
        // 如果startPosition为null，说明是第一次，默认+1处理
        if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            next = next + 1;
        }

        if (current >= maxAbleSequence) {
            return new Events<>();
        }

        Events<Event> result = new Events<>();
        List<Event> entries = result.getEvents();
        long memorySize = 0;
        if (batchMode.isItemSize()) {
            end = Math.min((next + batchSize - 1), maxAbleSequence);
            // 提取数据并返回
            for (; next <= end; next++) {
                Event event = buffer[getIndex(next)];
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entries.size() == 0) {
                        entries.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entries.add(event);
                }
            }
        } else {
            long maxMemorySize = batchSize * bufferMemUnit;
            for (; memorySize <= maxMemorySize && next <= maxAbleSequence; next++) {
                // 永远保证可以取出第一条的记录，避免死锁
                Event event = buffer[getIndex(next)];
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entries.size() == 0) {
                        entries.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entries.add(event);
                    memorySize += calculateSize(event);
                    end = next;// 记录end位点
                }
            }

        }

        PositionRange<LogPosition> range = new PositionRange<>();
        result.setPositionRange(range);

        range.setStart(CanalEventUtils.createPosition(entries.get(0)));
        range.setEnd(CanalEventUtils.createPosition(entries.get(result.getEvents().size() - 1)));
        range.setEndSeq(end);
        // 记录一下是否存在可以被ack的点

        for (int i = entries.size() - 1; i >= 0; i--) {
            Event event = entries.get(i);
            // GTID模式,ack的位点必须是事务结尾,因为下一次订阅的时候mysql会发送这个gtid之后的next,如果在事务头就记录了会丢这最后一个事务
            if ((CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntryType() && StringUtils.isEmpty(event.getGtid()))
                || CanalEntry.EntryType.TRANSACTIONEND == event.getEntryType() || isDdl(event.getEventType())) {
                // 将事务头/尾设置可被为ack的点
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
        }

        if (getSequence.compareAndSet(current, end)) {
            getMemorySize.addAndGet(memorySize);
            profiling(result.getEvents(), OP.GET);
            return result;
        } else {
            return new Events<>();
        }
    }

    @Override
    public Position getFirstPosition() throws CanalStoreException {
        long firstSequence = ackSequence.get();
        if (firstSequence == INIT_SEQUENCE && firstSequence < putSequence.get()) {
            // 没有ack过数据
            Event event = buffer[getIndex(firstSequence + 1)]; // 最后一次ack为-1，需要移动到下一条,included
            // = false
            return CanalEventUtils.createPosition(event, false);
        } else if (firstSequence > INIT_SEQUENCE && firstSequence < putSequence.get()) {
            // ack未追上put操作
            Event event = buffer[getIndex(firstSequence)]; // 最后一次ack的位置数据,需要移动到下一条,included
            // = false
            return CanalEventUtils.createPosition(event, false);
        } else if (firstSequence > INIT_SEQUENCE && firstSequence == putSequence.get()) {
            // 已经追上，store中没有数据
            Event event = buffer[getIndex(firstSequence)]; // 最后一次ack的位置数据，和last为同一条，included
            // = false
            return CanalEventUtils.createPosition(event, false);
        } else {
            // 没有任何数据
            return null;
        }
    }

    @Override
    public Position getLatestPosition() throws CanalStoreException {
        long latestSequence = putSequence.get();
        if (latestSequence > INIT_SEQUENCE && latestSequence != ackSequence.get()) {
            Event event = buffer[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，最后一条未消费的数据
            return CanalEventUtils.createPosition(event, true);
        } else if (latestSequence > INIT_SEQUENCE && latestSequence == ackSequence.get()) {
            // ack已经追上了put操作
            Event event = buffer[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，included
            // =
            // false
            return CanalEventUtils.createPosition(event, false);
        } else {
            // 没有任何数据
            return null;
        }
    }

    @Override
    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    @Override
    public void ack(Position position, Long seqId) throws CanalStoreException {
        cleanUntil(position, seqId);
    }

    @Override
    public void cleanUntil(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    protected void cleanUntil(Position position, Long seqId) throws CanalStoreException {
        long sequence = ackSequence.get();
        long maxSequence = getSequence.get();

        boolean hasMatch = false;
        long memorySize = 0;
        // ack没有list，但有已存在的foreach，还是节省一下list的开销
        long localExecTime = 0L;
        int deltaRows = 0;
        if (seqId > 0) {
            maxSequence = seqId;
        }
        for (long next = sequence + 1; next <= maxSequence; next++) {
            Event event = buffer[getIndex(next)];
            if (localExecTime == 0 && event.getExecuteTime() > 0) {
                localExecTime = event.getExecuteTime();
            }
            deltaRows += event.getRowsCount();
            memorySize += calculateSize(event);
            if ((seqId < 0 || next == seqId) && CanalEventUtils.checkPosition(event, (LogPosition) position)) {
                // 找到对应的position，更新ack seq
                hasMatch = true;

                if (batchMode.isMemSize()) {
                    ackMemorySize.addAndGet(memorySize);
                    // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                    for (long index = sequence + 1; index < next; index++) {
                        buffer[getIndex(index)] = null;// 设置为null
                    }

                    // 考虑getFirstPosition/getLastPosition会获取最后一次ack的position信息
                    // ack清理的时候只处理entry=null，释放内存
                    Event lastEvent = buffer[getIndex(next)];
                    lastEvent.setEntry(null);
                    lastEvent.setRawEntry(null);
                }

                if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
                    ackTableRows.addAndGet(deltaRows);
                    if (localExecTime > 0) {
                        ackExecTime.lazySet(localExecTime);
                    }
                    return;
                }
            }
        }
        if (!hasMatch) {// 找不到对应需要ack的position
            throw new CanalStoreException("no match ack position" + position.toString());
        }
    }

    @Override
    public void rollback() throws CanalStoreException {
        getSequence.set(ackSequence.get());
        getMemorySize.set(ackMemorySize.get());
    }

    @Override
    public void cleanAll() throws CanalStoreException {
        putSequence.set(INIT_SEQUENCE);
        getSequence.set(INIT_SEQUENCE);
        ackSequence.set(INIT_SEQUENCE);

        putMemorySize.set(0);
        getMemorySize.set(0);
        ackMemorySize.set(0);
        buffer = null;
    }

    /**
     * 检查Buffer指定位点是否为空
     */
    protected boolean checkFreeSlotAt(final long sequence) {
        long wrapPoint = sequence - bufferSize;
        long minPoint = Math.min(getSequence.get(), ackSequence.get());
        if (wrapPoint > minPoint) {
            return false;
        }
        if (batchMode.isMemSize()) {
            long memorySize = putMemorySize.get() - ackMemorySize.get();
            return memorySize < bufferSize * bufferMemUnit;
        }
        return true;
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize
     */
    protected boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
                next = next + 1;// 少一条数据
            }

            return current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence;
        } else {
            // 处理内存大小判断
            long currentSize = getMemorySize.get();
            long maxAbleSize = putMemorySize.get();

            return maxAbleSize - currentSize >= batchSize * bufferMemUnit;
        }
    }


    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getRawLength();
    }

    private int getIndex(long sequence) {
        return (int) sequence & indexMask;
    }

    private boolean isDdl(CanalEntry.EventType type) {
        return type == CanalEntry.EventType.ALTER || type == CanalEntry.EventType.CREATE || type == CanalEntry.EventType.ERASE
               || type == CanalEntry.EventType.RENAME || type == CanalEntry.EventType.TRUNCATE || type == CanalEntry.EventType.CINDEX
               || type == CanalEntry.EventType.DINDEX;
    }

    private void profiling(List<Event> events, OP op) {
        long localExecTime = 0L;
        int deltaRows = 0;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                if (localExecTime == 0 && e.getExecuteTime() > 0) {
                    localExecTime = e.getExecuteTime();
                }
                deltaRows += e.getRowsCount();
            }
        }
        switch (op) {
            case PUT:
                putTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    putExecTime.lazySet(localExecTime);
                }
                break;
            case GET:
                getTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    getExecTime.lazySet(localExecTime);
                }
                break;
            case ACK:
                ackTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    ackExecTime.lazySet(localExecTime);
                }
                break;
            default:
                break;
        }
    }

    private enum OP {
        PUT, GET, ACK
    }

    // ================ setter / getter ==================
    public int getBufferSize() {
        return this.bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public AtomicLong getPutSequence() {
        return putSequence;
    }

    public AtomicLong getAckSequence() {
        return ackSequence;
    }

    public AtomicLong getPutMemorySize() {
        return putMemorySize;
    }

    public AtomicLong getAckMemorySize() {
        return ackMemorySize;
    }

    public BatchMode getBatchMode() {
        return batchMode;
    }

    public AtomicLong getPutExecTime() {
        return putExecTime;
    }

    public AtomicLong getGetExecTime() {
        return getExecTime;
    }

    public AtomicLong getAckExecTime() {
        return ackExecTime;
    }

    public AtomicLong getPutTableRows() {
        return putTableRows;
    }

    public AtomicLong getGetTableRows() {
        return getTableRows;
    }

    public AtomicLong getAckTableRows() {
        return ackTableRows;
    }

}

