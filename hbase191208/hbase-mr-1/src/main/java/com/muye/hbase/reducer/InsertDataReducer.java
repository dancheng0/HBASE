package com.muye.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author : gwh
 * @date : 2019-12-17 10:24
 **/
public class InsertDataReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //运行reducer，增加数据
        for (Put put : values) {
            if(!put.isEmpty()){
            context.write(NullWritable.get(),put);
            }
        }
    }
}
