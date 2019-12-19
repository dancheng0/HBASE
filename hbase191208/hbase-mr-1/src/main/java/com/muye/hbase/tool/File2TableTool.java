package com.muye.hbase.tool;

import com.muye.hbase.mapper.ReadFileMapper;
import com.muye.hbase.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author : gwh
 * @date : 2019-12-17 17:08
 **/
public class File2TableTool implements Tool {

    public int run(String[] args) throws Exception {
        //初始化job
        Job job = Job.getInstance();
        //通过找到给定类的来源来设置jar
        job.setJarByClass(File2TableTool.class);
        //  hbase(util) = > hbase(util)
        // hdfs =>hbase(util)

        //format 读数据
        Path path = new Path("hdfs://hadoop2:9000/data/student.csv");
        FileInputFormat.addInputPath(job,path);
        //mapper
        job.setMapperClass(ReadFileMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reducer
        TableMapReduceUtil.initTableReducerJob( "ns1:user",InsertDataReducer.class,job);

        return job.waitForCompletion(true)?JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
