package com.alibaba.otter.canal.store.memory.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.store.model.Event;

import java.net.InetSocketAddress;

public class EventMockUtil {

    public static final String MYSQL_ADDRESS = "127.0.0.1";
    public static final int MYSQL_PORT = 3306;
    public static final long SLAVE_ID = -1L;
    public static final long DEFAULT_EVENT_LENGTH = 1024L;
    public static final InetSocketAddress SOCKET_ADDRESS = new InetSocketAddress(MYSQL_ADDRESS, MYSQL_PORT);
    public static final LogIdentity LOG_IDENTITY = new LogIdentity(SOCKET_ADDRESS, SLAVE_ID);

    public static Event buildEvent(String binlogFile,
                                   long offset,
                                   long timestamp) {
        return EventMockUtil.buildEvent(binlogFile, offset, timestamp, DEFAULT_EVENT_LENGTH);
    }

    public static Event buildEvent(String binlogFile,
                                   long offset,
                                   long timestamp,
                                   long eventLength) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        headerBuilder.setEventLength(eventLength);
        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        CanalEntry.Entry entry = entryBuilder.build();
        return new Event(LOG_IDENTITY, entry);
    }

}
