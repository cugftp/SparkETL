package cc.lfwzc.cmd.SparkStreaming.writer;

import java.util.Map;

/**
 * 数据写入接口
 */
public interface SQLWriterInterface {
    boolean createTable(String tablename, Map<String, String> cols);
    void writeToTable(String tablename , Map<String,String> colAndData);
    void writeToDirtyTable(String tablename,String dirtyData);
}
