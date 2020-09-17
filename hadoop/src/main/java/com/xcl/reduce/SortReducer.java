package com.xcl.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//第一对kv:类型要与Mapper输出类型一致：Text, IntWritable
//第二队kv:自己设计决定输出的结果数据是什么类型：Text, IntWritable
public class SortReducer extends Reducer<LongWritable, LongWritable,LongWritable,LongWritable> {
    private static LongWritable sort = new LongWritable(1);
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.write(sort,key);
        sort = new LongWritable(sort.get() + 1);
    }
}
