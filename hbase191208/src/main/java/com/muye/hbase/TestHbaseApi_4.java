package com.muye.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 查询数据
 *
 * @author : gwh
 * @date : 2019-12-08 21:16
 *
 **/
public class TestHbaseApi_4 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);
        // 扫描数据
        Scan scan = new Scan();
        // scan.addFamily()
        // 二进制比较
        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("1001"));
        // 正则条件过滤
        RegexStringComparator regexStringComparator = new RegexStringComparator("^\\d{3}$");
        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,regexStringComparator);
        Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,binaryComparator);
        // FilterList.Operator.MUST_PASS_ALL and
        // FilterList.Operator.MUST_PASS_ONE or
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        list.addFilter(filter1);
        list.addFilter(filter2);
        // 扫描时，增加过滤器
        //所谓过滤，其实每条数据都会筛选过滤，性能比较低
        scan.setFilter(list);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("family:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("column:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }
        table.close();
        connection.close();
    }
}
