package com.bigdata.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.mr.LogFieldWritable;
import com.bigdata.mr.LogGenericWritable;
import com.bigdata.mr.LogOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HiveParseJob extends Configured implements Tool {

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //解析正确计数
            Counter successCounter = context.getCounter("Log success", "Parse success");
            try {
                successCounter.increment(1);
                JSONObject vo = JSON.parseObject(value.toString());
                context.write(new Text(vo.getString("user_id")), value);
            } catch (Exception e) {

            }
        }
    }

    public static class LogReducer extends Reducer<Text, Text, Text, Text> {

        public void setup(Context context) throws IOException {
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Counter OneUserCounter = context.getCounter("Log success", "OneUserCount");
            int size = 0;
            for (Text v : values) {
                size = size + 1;
            }
            OneUserCounter.increment(1);
            context.write(null, new Text("用户ID：" + key + "个数为：" + size));
        }

        public void cleanup(Context context) {

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //获取系统配置文件
        Configuration conf = getConf();
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        //加载自定义配置文件
        conf.addResource("mr.xml");
        //创建JOB
        Job job = Job.getInstance(conf);
        job.setJarByClass(HiveParseJob.class);
        job.setJobName("HiveParseJob");
        job.setMapperClass(HiveParseJob.LogMapper.class);
        job.setReducerClass(HiveParseJob.LogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        Configuration conf = new Configuration();
        int reint = ToolRunner.run(conf, new HiveParseJob(), args);
        System.exit(reint);
    }
}
