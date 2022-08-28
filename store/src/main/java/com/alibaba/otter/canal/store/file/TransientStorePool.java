package com.alibaba.otter.canal.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Deque;

public class TransientStorePool {

    protected static final Logger log = LoggerFactory.getLogger(TransientStorePool.class);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;



}
