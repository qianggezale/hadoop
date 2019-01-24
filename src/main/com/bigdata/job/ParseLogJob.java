package com.bigdata.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.mr.LogFieldWritable;
import com.bigdata.mr.LogGenericWritable;
import com.bigdata.utils.IPUtil;
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
import java.net.URI;
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

        public void setup(Context context) throws IOException {
//            //如果job中已经添加了分布式缓存此处代码不需要拷贝文件到本地了
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path IpPath = new Path(context.getConfiguration().get("ip.file.path"));
//            Path localPath = new Path(this.getClass().getResource("/").getPath());
//            fs.copyToLocalFile(IpPath, localPath);
            IPUtil.load(context.getConfiguration().get("db.filename"));
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {

            for (LogGenericWritable v : values) {
                String ip = v.getObject("ip").toString();
                String[] adds = IPUtil.find(ip);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("guojia", adds[0]);
                jsonObject.put("sheng", adds[1]);
                jsonObject.put("chengshi", adds[2]);

                JSONObject vo = JSON.parseObject(v.asJsonString());
                vo.put("address", jsonObject.toJSONString());

                context.write(null, new Text(vo.toJSONString()));
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

        FileInputFormat.addInputPath(job, new Path(conf.get("job.inputpath")));
        Path outpath = new Path(conf.get("job.outputpath"));
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
