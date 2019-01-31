package com.bigdata.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextLongPartitioner extends Partitioner<TextLongWritable, Writable> {

    @Override
    public int getPartition(TextLongWritable textLongWritable, Writable writable, int i) {
        int hash = textLongWritable.getText().hashCode();
        return (hash & Integer.MAX_VALUE) % i;
    }
}
