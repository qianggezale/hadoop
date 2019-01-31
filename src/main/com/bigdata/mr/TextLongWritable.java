package com.bigdata.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextLongWritable implements WritableComparable<TextLongWritable> {

    private Text text;
    private LongWritable longWritable;

    public Text getText() {
        return text;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public LongWritable getLongWritable() {
        return longWritable;
    }

    public void setLongWritable(LongWritable longWritable) {
        this.longWritable = longWritable;
    }

    public TextLongWritable() {
        this.text = new Text();
        this.longWritable = new LongWritable(0);
    }

    public int hashCode() {
        final int prime = 31;
        return this.text.hashCode() * prime + this.longWritable.hashCode() * prime;
    }

    public int compareTo(TextLongWritable o) {
        int result = this.text.compareTo(o.getText());
        if (result == 0) {
            result = this.longWritable.compareTo(o.getLongWritable());
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.text.write(dataOutput);
        WritableUtils.writeVLong(dataOutput, this.longWritable.get());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.text.readFields(dataInput);
        this.longWritable.set(WritableUtils.readVLong(dataInput));
    }
}
