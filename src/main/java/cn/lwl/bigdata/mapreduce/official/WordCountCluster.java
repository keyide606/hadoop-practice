package cn.lwl.bigdata.mapreduce.official;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 打jar包到linux 以hadoop jar 的方式提交任务到yarn集群
 */
public class WordCountCluster {
    public static void main(String[] args) throws Exception {
        // 设置用户名为bigdata
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Configuration conf = new Configuration();
        // 帮忙解析启动jar包的参数,自动将-D参数解析到conf中,留下程序需要的参数
//        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
//        String[] options = genericOptionsParser.getRemainingArgs();
        Job job = Job.getInstance(conf);
        // 通过Class对象反推jar包,阅读源码观察
        job.setJarByClass(WordCountCluster.class);
        // Specify various job-specific parameters
        job.setJobName("wc");

        // 设置mapper相关类
        job.setMapperClass(WordCountMapper.class);
        // 设置mapper的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置reducer相关类
        job.setReducerClass(WordCountReducer.class);


        // 不在这样写了,因为可能计算的源文件来自其他数据源
//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        Path input = new Path("/data/test/wordcount/input");
        TextInputFormat.addInputPath(job, input);

        Path output = new Path("/data/test/wordcount/output");
        TextOutputFormat.setOutputPath(job, output);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
