package com.xcl.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
// Mapper类的泛型参数：共4个，2对kv
//第一对kv:map输入参数类型
//第二队kv：map输出参数类型
// LongWritable, Text-->文本偏移量，一行文本内容
public class SortMapper extends Mapper<LongWritable, Text,LongWritable,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(Long.parseLong(value.toString())),new LongWritable(1));
    }
}
