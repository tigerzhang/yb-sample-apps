package com.yugabyte.sample.common;

import java.util.concurrent.atomic.AtomicLong;

public class YunbaSubLoadGenerator {
    private AtomicLong maxUid;

    public YunbaSubLoadGenerator() {
        maxUid = new AtomicLong();
    }

    public SubData getReadSubData() {
        long id = maxUid.longValue();
        SubData subData = new SubData();
        subData.appkey = String.format("appkey%d", id/100000);
        subData.id = new Long(id).toString();
        subData.topic = String.format("topic%d", id/100);
        subData.platform = (int)(id%3);
        subData.platform = (int)(id%3);
        return subData;
    }

    public SubData getWriteSubData() {
        long id = maxUid.incrementAndGet();
        SubData subData = new SubData();
        subData.appkey = String.format("appkey%d", id/100000);
        subData.id = new Long(id).toString();
        subData.topic = String.format("topic%d", id/100);
        subData.platform = (int)(id%3);
        subData.platform = (int)(id%3);
        return subData;
    }

    public void recordWriteSuccess(String id) {
    }

    public void recordWriteFailure(String id) {
    }

    public class AppkeyAndTopic {
        public String appkey;
        public String topic;
    }

    public class SubData {
        public String appkey;
        public String id;
        public String topic;
        public int platform;
        public int qos;
    }
}
