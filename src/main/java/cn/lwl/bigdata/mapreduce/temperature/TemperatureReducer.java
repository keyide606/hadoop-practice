package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<TemperatureWritable, IntWritable, Text, IntWritable> {

    private Text text = new Text();
    private IntWritable intWritable = new IntWritable();

    @Override
    public void reduce(TemperatureWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        for (IntWritable value : values) {
            if (flag < 2) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(key.getYear());
                stringBuilder.append("-");
                stringBuilder.append(key.getMonth());
                stringBuilder.append("-");
                stringBuilder.append(key.getDay());
                System.out.println(stringBuilder.toString());
                text.set(stringBuilder.toString());
                intWritable.set(value.get());
                context.write(text, intWritable);
                flag++;
            } else {
                break;
            }
        }
    }
}
