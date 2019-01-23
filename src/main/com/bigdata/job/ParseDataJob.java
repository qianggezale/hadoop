package com.bigdata.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class ParseDataJob extends Configured implements Tool {


    public static Text parseLog(String row) throws ParseException {
        String[] logpart = StringUtils.split(row, "|");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logpart[0]).getTime();
        String activeName = logpart[1];
        JSONObject json2 = JSON.parseObject(logpart[2]);

        JSONObject returnJson = new JSONObject();
        returnJson.put("time_tag", timeTag);
        returnJson.put("active_name", activeName);
        returnJson.putAll(json2);
        return new Text(returnJson.toJSONString());
    }

    public static class LogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                Text logs = parseLog(value.toString());
                context.write(null, logs);
            } catch (ParseException e) {
                e.printStackTrace();
            }
//            super.map(key, value, context);
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseDataJob.class);
        job.setJobName("ParseDataJob");
        job.setMapperClass(LogMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

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
        int reint = ToolRunner.run(new Configuration(), new ParseDataJob(), args);
        System.exit(reint);
    }
}
