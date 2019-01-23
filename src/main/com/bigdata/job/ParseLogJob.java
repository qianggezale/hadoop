package com.bigdata.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.mr.LogFieldWritable;
import com.bigdata.mr.LogGenericWritable;
import org.anarres.lzo.hadoop.codec.LzoCodec;
import org.anarres.lzo.hadoop.codec.LzopCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool {

    public static class LogWritable extends LogGenericWritable {
        public String[] getFields() {
            return new String[]{"device_id", "user_id", "time_tag", "active_name", "ip", "session_id", "req_url"};
        }
    }

    public static LogGenericWritable parseLog(String row) throws ParseException {
        String[] logpart = StringUtils.split(row, "|");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logpart[0]).getTime();
        String activeName = logpart[1];
        JSONObject json2 = JSON.parseObject(logpart[2]);

        LogWritable lw = new LogWritable();
        lw.put("time_tag", new LogFieldWritable(timeTag));
        lw.put("active_name", new LogFieldWritable(activeName));
        for (Map.Entry<String, Object> entry : json2.entrySet()) {
            lw.put(entry.getKey(), new LogFieldWritable(entry.getValue()));
        }
        return lw;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogGenericWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                LogGenericWritable logs = parseLog(value.toString());
                context.write(key, logs);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, LogGenericWritable, NullWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {

            for (LogGenericWritable v : values) {
                context.write(null, new Text(v.asJsonString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("ParseLogJob");
        job.setMapperClass(ParseLogJob.LogMapper.class);
        job.setReducerClass(ParseLogJob.LogReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
//        //输出Lzop压缩
//        FileOutputFormat.setCompressOutput(job,true);
//        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始执行");
        int reint = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(reint);
    }

}
