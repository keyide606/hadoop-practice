package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureSortComparator extends WritableComparator {

    public TemperatureSortComparator() {
        super(TemperatureWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 转换成所需类
        TemperatureWritable one = (TemperatureWritable) a;
        TemperatureWritable two = (TemperatureWritable) b;
        // 开始比较,年 月 温度 比较，按照温度降序
        int compare = Integer.compare(one.getYear(), two.getYear());
        if (compare != 0) {
            return compare;
        }
        compare = Integer.compare(one.getMonth(), two.getMonth());
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(two.getTemperature(), one.getTemperature());
    }
}
