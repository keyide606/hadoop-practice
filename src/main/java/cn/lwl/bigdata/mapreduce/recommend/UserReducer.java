package cn.lwl.bigdata.mapreduce.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int flag = 0;
        int sum = 0;
        for (IntWritable value : values) {

            if (value.get() == 1) {
                flag = 1;
                break;
            }
            sum += 1;
        }
        if (flag == 0) {
            IntWritable result = new IntWritable();
            result.set(sum);
            context.write(key, result);
        }

    }
}
