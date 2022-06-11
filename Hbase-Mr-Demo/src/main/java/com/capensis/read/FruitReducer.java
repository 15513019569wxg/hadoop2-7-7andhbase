package com.capensis.read;/*
    @author wxg
    @date 2021/7/18-0:44
    */

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import sun.java2d.pipe.NullPipe;

import java.io.IOException;

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
    String cf1 = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        cf1 = configuration.get("cf1");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //  1、遍历values
        for (Text value : values) {
            //  2、获取每一行数据
            String[] fields = value.toString().split("\t");
            //  3、构建put对象
            Put put = new Put(Bytes.toBytes(fields[0]));
            //  4、给put对象赋值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"),Bytes.toBytes(fields[2]));
            //  5、写出
            context.write(NullWritable.get(), put);

        }
    }
}
