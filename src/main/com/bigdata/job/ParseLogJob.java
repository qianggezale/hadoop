package com.bigdata.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.mr.LogFieldWritable;
import com.bigdata.mr.LogGenericWritable;
import com.bigdata.mr.LogOutputFormat;
import com.bigdata.utils.IPUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool {

    public static class LogWritable extends LogGenericWritable {
        public String[] getFields() {
            return new String[]{"device_id", "user_id", "time_tag", "active_name", "ip", "session_id", "req_url", "product_id", "order_id", "error_flag", "error_log"};
        }
    }

    public static LogGenericWritable parseLog(String row) throws Exception {
        String[] logpart = StringUtils.split(row, "\u1111");
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

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //解析错误计数
            Counter errorCounter = context.getCounter("Log error", "Parse error");
            //解析正确计数
            Counter successCounter = context.getCounter("Log success", "Parse success");
            try {
                LogGenericWritable logs = parseLog(value.toString());
//                //用userid为key判断每个key的个数
//                if (logs.getObject("user_id") == null) {
//                    context.write(new LongWritable(0), logs);
//                } else {
//                    long userid = (long) logs.getObject("user_id");
//                    context.write(new LongWritable(userid), logs);
//                }
                context.write(key, logs);
                successCounter.increment(1);
            } catch (Exception e) {
                errorCounter.increment(1);
                LogGenericWritable v = new LogWritable();
                v.put("error_flag", new LogFieldWritable("error"));
                v.put("error_log", new LogFieldWritable(value));
//                int outkey=(int)(Math.random()*100);
//                outkey.set
                context.write(key, v);
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, LogGenericWritable, Text, Text> {

        public void setup(Context context) throws IOException {
//            //如果job中已经添加了分布式缓存此处代码不需要拷贝文件到本地了
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path IpPath = new Path(context.getConfiguration().get("ip.file.path"));
//            Path localPath = new Path(this.getClass().getResource("/").getPath());
//            fs.copyToLocalFile(IpPath, localPath);
//            IPUtil.load(context.getConfiguration().get("db.filename"));
            IPUtil.load("17monipdb.dat");
        }

        protected void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {
            Counter OneUserCounter = context.getCounter("Log success", "OneUserCount");
            OneUserCounter.increment(1);
            for (LogGenericWritable v : values) {
                JSONObject vo = JSON.parseObject(v.asJsonString());
                if (v.getObject("error_flag") == null) {
                    String ip = (String) v.getObject("ip");
                    JSONObject jsonObject = new JSONObject();
                    String[] adds = IPUtil.find(ip);
                    jsonObject.put("guojia", adds[0]);
                    jsonObject.put("sheng", adds[1]);
                    jsonObject.put("chengshi", adds[2]);
                    vo.put("address", jsonObject.toJSONString());
                }
                String keyout = v.getObject("error_flag") == null ? "part" : "error/part";
                context.write(new Text(keyout), new Text(vo.toJSONString()));
            }
        }

        public void cleanup(Context context) {

        }
    }

    public int run(String[] args) throws Exception {
        //获取系统配置文件
        Configuration conf = getConf();
        //加载自定义配置文件
        conf.addResource("mr.xml");
        //创建JOB
        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("ParseLogJob");
        job.setMapperClass(ParseLogJob.LogMapper.class);
        job.setReducerClass(ParseLogJob.LogReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputValueClass(Text.class);
        //文件放到分布式缓存中
        job.addCacheFile(new URI(conf.get("ip.file.path")));
//        //设置多个小文件在一个map中
//        job.setInputFormatClass(CombineTextInputFormat.class);

        job.setOutputFormatClass(LogOutputFormat.class);

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
        Configuration conf = new Configuration();
        int reint = ToolRunner.run(conf, new ParseLogJob(), args);
        System.exit(reint);
    }

}
