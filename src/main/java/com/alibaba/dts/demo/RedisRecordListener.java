package com.alibaba.dts.demo;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.*;
import com.aliyun.dts.subscribe.clients.record.value.Value;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisRecordListener implements RecordListener {
    // 连接池
    private static JedisPool pool = null;

    private static String redisUrl;

    private static int redisPort;

    private static String redisPassword;

    public RedisRecordListener(String redisUrl, int redisPort, String redisPassword){
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
    }

    public static Jedis getJedisClient() {
        if (null == pool) {
            JedisPoolConfig config = new JedisPoolConfig();
            if (StringUtils.isNotEmpty(redisPassword)) {
                pool = new JedisPool(config, redisUrl, redisPort, 3000, redisPassword, 0);
            } else {
                pool = new JedisPool(config, redisUrl, redisPort, 3000);
            }

            // pool = new JedisPool(config, redisUrl, redisPort,
            // 3000,redisPassword);
        }
        return pool.getResource();
    }

    @Override
    public void consume(DefaultUserRecord record) {
        OperationType operationType = record.getOperationType();

        if (operationType.equals(OperationType.INSERT)) {
            // 后镜像值按照依次排列

            RowImage rowImage = record.getAfterImage();
            Map<String, String> fieldValues = convertFieldValues(record, rowImage);

            if (null != fieldValues) {
                String key = createKey(record, rowImage);
                replicateInsertRecord(key, fieldValues);
            }
        } else if (operationType.equals(OperationType.UPDATE)) {

            RowImage beforeRowImage = record.getBeforeImage();

            RowImage afterRowImage = record.getAfterImage();
            Map<String, String> afterFieldValues = convertFieldValues(record, afterRowImage);

            if (null != afterFieldValues) {
                String oldKey = createKey(record, beforeRowImage);
                String newKey = createKey(record, afterRowImage);
                replicateUpdateRecord(newKey, afterFieldValues, oldKey);
            }
        } else if (operationType.equals(OperationType.DELETE)) {
            RowImage beforeRowImage = record.getBeforeImage();

            // 前镜像值按照依次排列
            String oldKey = createKey(record, beforeRowImage);
            replicateDeleteRecord(oldKey);
        } else {
            System.out.println("filter one record op=" + operationType + " , timestamp=" + record.getSourceTimestamp());
        }
    }

    // 解析所有字段，获取Map ==> Redis hash 类型
    public static Map<String, String> convertFieldValues(DefaultUserRecord record, RowImage rowImage) {
        if (null == record) {
            return null;
        }
        Map<String, String> fieldValues = new HashMap<String, String>();

        RecordSchema recordSchema = record.getSchema();

        for (RecordField recordField : recordSchema.getFields()) {
            Value value = RecordTools.getFiledValue(recordField.getFieldName(), rowImage);
            fieldValues.put(recordField.getFieldName(), value.toString());
        }

        return fieldValues.isEmpty() ? null : fieldValues;
    }

    public static String createKey(DefaultUserRecord record, RowImage rowImage) {
        RecordSchema recordSchema = record.getSchema();

        String dbName = recordSchema.getDatabaseName().get();
        String tbName = recordSchema.getTableName().get();
        StringBuilder key = new StringBuilder();
        key.append(dbName).append(":").append(tbName);

        List<RecordField> primaryRecordFields = RecordTools.getPrimary(recordSchema);

        //has primary key
        if (!primaryRecordFields.isEmpty()) {
            for (RecordField recordField : primaryRecordFields) {
                Value value = RecordTools.getFiledValue(recordField.getFieldName(), rowImage);
                key.append(":").append(value.toString());
            }
        } else {
            for (RecordField recordField : recordSchema.getFields()) {
                Value value = RecordTools.getFiledValue(recordField.getFieldName(), rowImage);
                key.append(":").append(value.toString());
            }
        }

        return key.toString();
    }

    // 插入一条数据
    public static void replicateInsertRecord(String key, Map<String, String> hash) {
        if (null == key || null == hash) {
            return;
        }
        System.out.println("Insert one key=" + key + " , value=" + hash);
        Jedis jedis = getJedisClient();
        jedis.hmset(key, hash);
    }

    // 修改一条数据,
    // 1.删除旧镜像值对应key.
    // 2.插入新镜像值
    public static void replicateUpdateRecord(String newKey, Map<String, String> newHash, String oldKey) {
        System.out.println("Update one oldKey=" + oldKey + " , newKey=" + newKey + " , value=" + oldKey);
        Jedis jedis = getJedisClient();
        jedis.del(oldKey);
        jedis.hmset(newKey, newHash);
    }

    // 删除一条数据
    public static void replicateDeleteRecord(String key) {
        if (null == key) {
            return;
        }
        Jedis jedis = getJedisClient();
        System.out.println("Delete one key=" + key);
        jedis.del(key);
        jedis.close();
    }
}
