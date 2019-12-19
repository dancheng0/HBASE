package com.muye.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author : gwh
 * @date : 2019-12-08 21:16
 **/
public class TestHbaseApi_1 {
    public static void main(String[] args) throws IOException {
        //0) 创建配置对象，获取HBASE连接
        //classpath:hbase-default.xml,hbase-site.xml
        Configuration configuration = HBaseConfiguration.create();
        //1) 获取操作对象：Admin
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);
        //2)获取操作对象 Admin
        //new HBaseAdmin(connection)
        Admin admin = connection.getAdmin();
        //3) 操作数据库:判断HBASE中是否存在某张表
        // 3-1) 判断命名空间是否存在
        NamespaceDescriptor namespace = null;
        try {
            namespace = admin.getNamespaceDescriptor("ns2");
        } catch (NamespaceNotFoundException e) {
            e.printStackTrace();
        }

        if (namespace == null) {
             admin.createNamespace(namespace);
        }
        TableName tableName = TableName.valueOf("ns1:student");
        boolean flag = admin.tableExists(tableName);
        System.out.println(flag);
        if (flag) {
            //获取指定的表对象
            Table table = connection.getTable(tableName);
            //查询数据
            String rowkey = "1001";
            //字符编码
            //String => byte[]  这个转换需要指定编码，使用工具类
            Get get = new Get(Bytes.toBytes(rowkey)) ;
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            System.out.println("1001数据是否存在："+empty);
            if(empty){
                //增加数据
                Put put = new Put(Bytes.toBytes(rowkey));
                String family = "info";
                String column = "name";
                String value = "zhangsan";
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                table.put(put);
                System.out.println("增加数据。。。");
            }else {
                //展示数据
               for (Cell cell:result.rawCells()){
                   System.out.println("value:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                   System.out.println("value:"+CellUtil.cloneValue(cell));
                   System.out.println("value:"+CellUtil.cloneQualifier(cell));
                   System.out.println("value:"+CellUtil.cloneRow(cell));
               }
            }
        } else {
            //创建表

            //创建表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addCoprocessor("com.muye.hbase.InsertStudentCorprocesser");

            //增加列族
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(hColumnDescriptor);

            admin.createTable(hTableDescriptor);
            System.out.println("表格创建成功");
            //4)获取操作结果
            //5)关闭数据库连接
        }

    }
}
