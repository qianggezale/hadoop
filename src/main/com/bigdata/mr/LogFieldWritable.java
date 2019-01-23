package com.bigdata.mr;

import org.apache.hadoop.io.*;

public class LogFieldWritable extends GenericWritable {

    public LogFieldWritable() {
        set(NullWritable.get());
    }

    public LogFieldWritable(Writable w) {
        set(w);
    }

    protected Class<? extends Writable>[] getTypes() {
        return new Class[]{Text.class, LongWritable.class, DoubleWritable.class, NullWritable.class};
    }

    public LogFieldWritable(Object object) {
        if (object == null) {
            set(NullWritable.get());
        } else if (object instanceof Writable) {
            set((Writable) object);
        } else if (object instanceof Long) {
            set(new LongWritable((Long) object));
        } else if (object instanceof Double) {
            set(new DoubleWritable((Double) object));
        } else if (object instanceof String) {
            set(new Text((String) object));
        } else {
            throw new RuntimeException("类型错误！");
        }
    }

    public Object getObject() {
        Writable w = get();
        if (w instanceof Text) {
            return w.toString();
        } else if (w instanceof DoubleWritable) {
            return ((DoubleWritable) w).get();
        } else if (w instanceof LongWritable) {
            return ((LongWritable) w).get();
        } else {
            return null;
        }
    }
}
