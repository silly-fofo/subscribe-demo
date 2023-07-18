package com.alibaba.dts.demo;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.DTSConsumer;
import com.aliyun.dts.subscribe.clients.DefaultDTSConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class DTSMySQL2RedisDemo {
    private static final Logger log = LoggerFactory.getLogger(DTSMySQL2RedisDemo.class);

    private final DTSConsumer dtsConsumer;

    public DTSMySQL2RedisDemo(String brokerUrl, String topic, String sid, String userName, String password,
                              String checkpoint, ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint) {
        this.dtsConsumer = initDTSClient(brokerUrl, topic, sid, userName, password, checkpoint, subscribeMode, isForceUseInitCheckpoint);
    }

    private DTSConsumer initDTSClient(String brokerUrl, String topic, String sid, String userName, String password,
                                      String initCheckpoint, ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint) {
        ConsumerContext consumerContext = new ConsumerContext(brokerUrl, topic, sid, userName, password, initCheckpoint, subscribeMode);

        //if this parameter is set, force to use the initCheckpoint to initial
        consumerContext.setForceUseCheckpoint(isForceUseInitCheckpoint);

        DTSConsumer dtsConsumer = new DefaultDTSConsumer(consumerContext);

        return dtsConsumer;
    }

    public void addRedisRecordListener(String redisUrl, int redisPort, String redisPassword) {
        this.dtsConsumer.addRecordListeners(Collections.singletonMap("RedisRecordPrinter", new RedisRecordListener(redisUrl, redisPort, redisPassword)));
    }

    public void start() {
        System.out.println("Start DTS subscription client...");

        dtsConsumer.start();
    }

    public static void main(String[] args) {
        // kafka broker url
        String brokerUrl = "your broker url";
        // topic to consume, partition is 0
        String topic = "your dts topic";
        // user password and sid for auth
        String sid = "your sid";
        String userName = "your user name";
        String password = "your password";
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        String initCheckpoint = "start timestamp";
        // when use subscribe mode, group config is required. kafka consumer group is enabled
        ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.ASSIGN;
        // if force use config checkpoint when start. for checkpoint reset, only assign mode works
        boolean isForceUseInitCheckpoint = true;

        DTSMySQL2RedisDemo consumerDemo = new DTSMySQL2RedisDemo(brokerUrl, topic, sid, userName, password, initCheckpoint, subscribeMode, isForceUseInitCheckpoint);

        //redis url and port
        String redisUrl = "your redis url";
        int redisPort = 6379;//replace to you redis port
        String redisPassword = "your redis password";

        consumerDemo.addRedisRecordListener(redisUrl, redisPort, redisPassword);

        consumerDemo.start();
    }
}
