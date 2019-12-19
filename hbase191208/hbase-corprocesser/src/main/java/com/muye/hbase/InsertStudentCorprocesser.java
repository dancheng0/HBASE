package com.muye.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器 （HBase 自己的功能）
 * 1）创建类，继承BaseRegionObserver
 * 2) 重写方法：postPut
 * 3）实现逻辑: 增加student的数据，同时增加ns1:student中数据
 * 4) 将项目打包（依赖）后上传到HBASE中，让HBASE可以识别协处理器
 * 5） 删除原始表，增加新表时，同时设定协处理器
 * @author : gwh
 * @date : 2019-12-18 14:19
 **/
public class InsertStudentCorprocesser extends BaseRegionObserver {
   //prePut
    //doPut
    //postPut

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
//        super.postPut(e, put, edit, durability);

        //获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("student"));

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

}
