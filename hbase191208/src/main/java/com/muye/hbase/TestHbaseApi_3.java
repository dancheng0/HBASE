package com.muye.hbase;

import com.muye.util.HBaseUtil;
import java.io.IOException;

/**
 * @author : gwh
 * @date : 2019-12-08 21:16
 *
 **/
public class TestHbaseApi_3 {
    public static void main(String[] args) throws IOException {
       //创建连接
        HBaseUtil.makeConnection();
        //获取表
//        HBaseUtil.listTable();
        //获取命名空间下的表
        HBaseUtil.listTableWithNameSpace("ns1");
        //创建命名空间
//        HBaseUtil.createNameSpace("ns2");
        //插入数据
//        HBaseUtil.insertData("ns1:2", "1002", "info", "name", "lisi");
        //关闭链接
        HBaseUtil.close();
    }
}
