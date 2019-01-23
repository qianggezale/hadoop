package com.bigdata.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

abstract public class LogGenericWritable implements Writable {
    LogFieldWritable[] fields;
    String[] fnames;
    Map<String, Integer> mapIndex;

    abstract public String[] getFields();

    public LogGenericWritable() {
        fnames = getFields();
        if (fnames == null) {
            throw new RuntimeException("字段未初始化！");
        }
        mapIndex = new HashMap<String, Integer>();
        for (int i = 0; i < fnames.length; i++) {
            if (mapIndex.containsKey(fnames[i])) {
                throw new RuntimeException("此字段已经存在！");
            }
            mapIndex.put(fnames[i], i);
        }
        fields = new LogFieldWritable[fnames.length];
        for (int i = 0; i < fnames.length; i++) {
            fields[i] = new LogFieldWritable();
        }
    }

    private Integer getIndexByName(String name) {
        Integer index = mapIndex.get(name);
        if (index == null) {
            throw new RuntimeException("未找到该名称：" + name);
        }
        return index;
    }

    public void put(String name, LogFieldWritable value) {
        int index = getIndexByName(name);
        fields[index] = value;
    }

    public Writable getWritable(String name) {
        int index = getIndexByName(name);
        return fields[index];
    }

    public Object getObject(String name) {
        int index = getIndexByName(name);
        return fields[index].getObject();
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVInt(dataOutput, fnames.length);
        for (int i = 0; i < fnames.length; i++) {
            fields[i].write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        int count = WritableUtils.readVInt(dataInput);
        fields = new LogFieldWritable[count];
        for (int i = 0; i < count; i++) {
            LogFieldWritable value = new LogFieldWritable();
            value.readFields(dataInput);
            fields[i] = value;
        }
    }

    public String asJsonString() {
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < fnames.length; i++) {
            jsonObject.put(fnames[i], fields[i].getObject());
        }
        return jsonObject.toJSONString();
    }
}
