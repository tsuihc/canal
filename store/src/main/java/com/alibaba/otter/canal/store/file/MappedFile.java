package com.alibaba.otter.canal.store.file;

import java.io.IOError;
import java.io.IOException;
import java.nio.channels.FileChannel;

public interface MappedFile {

    /**
     * @return the file name of the {@link MappedFile}
     */
    String getFileName();

    /**
     * @return the {@link FileChannel} behind the {@link MappedFile}
     */
    FileChannel getFileChannel();

    /**
     * @return true if the file is full
     */
    boolean isFull();

    /**
     * @return true if the file is available
     */
    boolean isAvailable();

    /**
     * Increases the reference count
     *
     * @return true if success
     */
    boolean hold();

    /**
     * Decreases the reference count
     */
    void release();

    /**
     * Init the mapped file
     * @param fileName file name
     * @param fileSize file size
     * @param transientStorePool transient store pool
     * @throws IOException if any exceptions
     */
    void init(
            String fileName,
            int fileSize,
            TransientStorePool transientStorePool
    ) throws IOException;

}
