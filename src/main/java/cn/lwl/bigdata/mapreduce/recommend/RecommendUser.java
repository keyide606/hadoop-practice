package cn.lwl.bigdata.mapreduce.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RecommendUser {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration(true);

        Job job = Job.getInstance(configuration);
        job.setJobName("recommend-user");
        job.setJarByClass(RecommendUser.class);

        job.setMapperClass(UserMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(UserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("data/friend.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\tmp\\output"));

        job.waitForCompletion(true);
    }
}
