package com.muye.hbase;

import com.muye.hbase.tool.HbaseMrTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author : gwh
 * @date : 2019-12-16 21:18
 **/
public class Table2TableApplicatioin {
    public static void main(String[] args) throws Exception {
        // ToolRunner 可以运行mr
        ToolRunner.run(new HbaseMrTool(), args);
    }
}
