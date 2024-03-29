package com.muye.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author : gwh
 * @date : 2019-12-08 21:16
 **/
public class TestHbaseApi_2 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:student");
        Table table = connection.getTable(tableName);
        //删除表
        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)){
            //禁用表
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
        }

        //删除数据
       /* Delete delete = new Delete(Bytes.toBytes("1002"));
        table.delete(delete);
        System.out.println("删除成功");
*/
       //扫描数据
      /*  Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("family:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("column:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }*/


    }
}
