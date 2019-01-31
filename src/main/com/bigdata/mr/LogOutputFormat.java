package com.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LogOutputFormat<K, V> extends TextOutputFormat<K, V> {

    //数字格式化
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    RecordWriter<K, V> writer = null;


    public Path getTaskOutputPath(TaskAttemptContext job) throws IOException {
        Path outpath;
        OutputCommitter committer = getOutputCommitter(job);
        if (committer instanceof FileOutputCommitter) {
            outpath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path temppath = getOutputPath(job);
            if (temppath == null) {
                throw new IOException("getOutputPath Is Null ！");
            }
            outpath = temppath;
        }
        return outpath;
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        if (writer == null) {
            writer = new MyRecrdWriter(job, getTaskOutputPath(job));
        }
        return writer;
    }

    public class MyRecrdWriter extends RecordWriter<K, V> {

        private Map<String, RecordWriter<K, V>> mapWirters;
        private TaskAttemptContext job;
        private Path outpath;

        public MyRecrdWriter(TaskAttemptContext job, Path path) {
            super();
            this.job = job;
            this.outpath = path;
            this.mapWirters = new HashMap<String, RecordWriter<K, V>>();
        }

        public String getFileBaseName(K key, String Name) {
            return new StringBuilder(60).append(key.toString()).append("-").append(Name).toString();
        }

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
            TaskID taskID = job.getTaskAttemptID().getTaskID();
            int partition = taskID.getId();
            String baseName = getFileBaseName(k, NUMBER_FORMAT.format(partition));
            RecordWriter<K, V> rw = this.mapWirters.get(baseName);
            if (rw == null) {
                rw = getBaseWriter(job, baseName);
                mapWirters.put(baseName, rw);
            }
            rw.write(null, v);
        }

        public RecordWriter<K, V> getBaseWriter(TaskAttemptContext job, String baseName) throws IOException {
            RecordWriter<K, V> rw;
            //判断压缩格式
            boolean isya = getCompressOutput(job);
            Configuration conf = job.getConfiguration();
            //压缩格式解析
            if (isya) {
                Class<? extends CompressionCodec> codeClass = getOutputCompressorClass(job, GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codeClass, conf);
                Path file = new Path(outpath, baseName + codec.getDefaultExtension());
                FSDataOutputStream fileout = file.getFileSystem(conf).create(file);
                rw = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileout)));
            } else {
                Path file = new Path(outpath, baseName);
                FSDataOutputStream fileout = file.getFileSystem(conf).create(file, false);
                rw = new LineRecordWriter<K, V>(fileout);
            }
            return rw;
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> values = this.mapWirters.values().iterator();
            while (values.hasNext()) {
                values.next().close(taskAttemptContext);
            }
            this.mapWirters.clear();
        }
    }
}
