package com.alibaba.otter.canal.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMappedFile extends AbstractMappedFile {

    public static final int OS_PAGE_SIZE = 1024 & 4; // 4M

    protected static final Logger log = LoggerFactory.getLogger(DefaultMappedFile.class);

    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong();
    protected static final AtomicLong TOTAL_MAPPED_FILES = new AtomicLong();

    protected final AtomicInteger wrotePosition = new AtomicInteger();
    protected final AtomicInteger committedPosition = new AtomicInteger();
    protected final AtomicInteger flushedPosition = new AtomicInteger();

    protected int fileSize;
    protected String fileName;
    protected FileChannel fileChannel;
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;
    protected long fileFromOffset;
    protected File file;
    protected volatile long storeTimestamp = 0L;
    protected boolean firstCreateInQueue = false;
    protected long lastFlushTimestamp = -1L;
    protected MappedByteBuffer mappedByteBuffer;
    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTimestamp = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(
            String fileName,
            int fileSize
    ) {

    }

    @Override
    public void init(
            String fileName,
            int fileSize,
            TransientStorePool transientStorePool
    ) throws IOException {
        this.init(fileName, fileSize);
        this.transientStorePool = transientStorePool;
        this.writeBuffer = this.transientStorePool.borrowBuffer();
    }

    protected void init(
            String fileName,
            int fileSize
    ) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());

        Exception exception = null;
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
        } catch (Exception e) {
            exception = e;
            log.error("Failed to map file {}", fileName, e);
        } finally {
            if (exception != null) {
                if (this.fileChannel != null) {
                    this.fileChannel.close();
                }
                throw new IOException(exception);
            }
        }
    }

}
