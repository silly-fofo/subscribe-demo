package com.alibaba.dts.demo;

import com.aliyun.dts.subscribe.clients.record.RecordField;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.value.Value;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class RecordTools {

    public static List<RecordField> getPrimary(RecordSchema recordSchema) {
        List<RecordField> primaryKey = new LinkedList<>();
        if (recordSchema.getPrimaryIndexInfo() != null && !isEmpty(recordSchema.getPrimaryIndexInfo().getIndexFields())) {
            primaryKey = recordSchema.getPrimaryIndexInfo().getIndexFields();
        }

        return primaryKey;
    }

    public static boolean isEmpty(Collection<?> coll) {
        return coll == null || coll.isEmpty();
    }

    public static Value getFiledValue(String fieldName, RowImage rawImage) {
        return tryGetFieldValue(fieldName, rawImage);
    }

    public static Value tryGetFieldValue(String fieldName, RowImage rowImage) {
        return rowImage.getValue(fieldName);
    }
}
