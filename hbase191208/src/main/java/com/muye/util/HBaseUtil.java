package com.muye.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 *
 * HBASE 操作工具类
 *
 * @author : gwh
 * @date : 2019-12-16 11:25
 **/
public class HBaseUtil {

    /**
     * ThreadLocal 可以访问线程的内存空间  可以解决数据共享的问题
     */
    private static ThreadLocal<Connection>  connHolder = new ThreadLocal<Connection>();

    private HBaseUtil() {

    }


    /**
     * 获取HBASE的连接对象
     *
     * @return
     */
    public static void makeConnection() throws IOException {
        Connection conn = connHolder.get();
        if(conn==null){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }


    /**
     * 获取HBase中的表
     * */
    public static void listTable() throws IOException {
        Connection conn = connHolder.get();
        Admin admin = conn.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }
    }

    /**
     * 获取HBase中某个命名空间下的表
     * */
    public static void listTableWithNameSpace(String nameSpace) throws IOException {
        Connection conn = connHolder.get();
        Admin admin = conn.getAdmin();
        TableName[] ts = admin.listTableNamesByNamespace(nameSpace);
        for (TableName tableName : ts) {
            System.out.println(tableName.getNameWithNamespaceInclAsString());
        }
    }

    /**
     * 创建命名空间
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection conn = connHolder.get();
        Admin admin = conn.getAdmin();
        NamespaceDescriptor namespace = null;
        try {
            admin.getNamespaceDescriptor(nameSpace);
        } catch (NamespaceNotFoundException e) {
            //创建命名空间描述器
            namespace = NamespaceDescriptor.create(nameSpace).build();
            //管理对象创建命名空间
            admin.createNamespace(namespace);
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void createTable(String tableName,String family,String column,String value) throws IOException {
        Connection conn = connHolder.get();
        Admin admin = conn.getAdmin();
        //判断表是否存在
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表"+tableName+"已存在");
        }else{
            //创建表属性对象，表名需要转字节d
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建列族
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            //添加列族
            descriptor.addFamily(hColumnDescriptor);
            //创建表
            admin.createTable(descriptor);
        }
    }

    /**
     * 添加列族
     * @param tableName
     * @throws IOException
     */
    public static void createColumn(String tableName) throws IOException {
        Connection conn = connHolder.get();
        Admin admin = conn.getAdmin();
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("cf1"));
        admin.addColumn(TableName.valueOf(tableName), hColumnDescriptor);
    }
    

    /**
     * 插入数据
     * @param tableName
     * @param rowkey
     * @param family
     * @param column
     * @param value
     */
    public static void insertData(String tableName, String rowkey, String family, String column, String value) throws IOException {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        if(result.isEmpty()){
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
        }else{
            System.out.println("rowkey:"+rowkey+"已存在");
        }
    }

    /**
     * 关闭连接
     * @throws IOException
     */
    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
        }
    }

}
