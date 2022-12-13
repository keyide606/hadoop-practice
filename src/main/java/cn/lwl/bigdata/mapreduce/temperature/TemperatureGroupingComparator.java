package cn.lwl.bigdata.mapreduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureGroupingComparator extends WritableComparator {
    public TemperatureGroupingComparator() {
        super(TemperatureWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 转换成所需类
        TemperatureWritable one = (TemperatureWritable) a;
        TemperatureWritable two = (TemperatureWritable) b;
        // 分组时,按照年月分组
        int compare = Integer.compare(one.getYear(), two.getYear());
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(one.getMonth(), two.getMonth());
    }
}
