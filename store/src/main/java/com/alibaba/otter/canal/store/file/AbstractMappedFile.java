package com.alibaba.otter.canal.store.file;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMappedFile implements MappedFile {
    protected final AtomicLong refCount = new AtomicLong();
    protected volatile boolean available = true;
    protected volatile boolean cleanedUp = false;

    private volatile long firstShutdownTs = 0L;

    @Override
    public boolean isAvailable() {
        return this.available;
    }

    @Override
    public final synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }
        return false;
    }

    @Override
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value <= 0) {
            synchronized (this) {
                this.cleanedUp = this.cleanup(value);
            }
        }
    }

    public void shutdown(int intervalForcibly) {
        if (this.isAvailable()) {
            this.available = false;
            this.firstShutdownTs = System.currentTimeMillis();
            this.release();
        } else if (this.refCount.get() > 0 && intervalFromFirstShutdown() >= intervalForcibly) {
            this.refCount.set(-1000 - this.refCount.get());
            this.release();
        }
    }

    private long intervalFromFirstShutdown() {
        return System.currentTimeMillis() - firstShutdownTs;
    }

    public abstract boolean cleanup(long refCount);

}
