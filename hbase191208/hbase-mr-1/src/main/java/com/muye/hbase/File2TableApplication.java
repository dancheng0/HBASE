package com.muye.hbase;

import com.muye.hbase.tool.File2TableTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author : gwh
 * @date : 2019-12-17 17:06
 **/
public class File2TableApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new File2TableTool(), args);
    }
}
