package com.muye.hbase.tool;

import com.muye.hbase.mapper.ScanDataMapper;
import com.muye.hbase.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 从一张表里取向另外一张表中放
 *
 * @author : gwh
 * @date : 2019-12-16 21:20
 **/
public class HbaseMrTool implements Tool {
    public int run(String[] args) throws Exception {
        //Job
        Job job = Job.getInstance();
        //根据类来找jar的位置
        job.setJarByClass(HbaseMrTool.class);
        //mapper
        TableMapReduceUtil.initTableMapperJob(
                "ns1:student", new Scan(),ScanDataMapper.class ,ImmutableBytesWritable.class,Put.class, job);
        //reducer
        TableMapReduceUtil.initTableReducerJob( "ns1:user",InsertDataReducer.class,job);

        //执行作业
        boolean flg = job.waitForCompletion(true);
        return flg ? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
