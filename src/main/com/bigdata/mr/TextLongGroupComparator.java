package com.bigdata.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextLongGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextLongWritable textlong_a = (TextLongWritable) a;
        TextLongWritable textlong_b = (TextLongWritable) b;
        return textlong_a.getText().compareTo(textlong_b.getText());
    }

    public TextLongGroupComparator() {
        super(TextLongWritable.class, true);
    }
}
