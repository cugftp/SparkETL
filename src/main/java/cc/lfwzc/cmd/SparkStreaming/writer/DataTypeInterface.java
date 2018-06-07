package cc.lfwzc.cmd.SparkStreaming.writer;

/**
 * 数据类型匹配接口
 * 用于获取不同数据库的类型名称
 */
public interface DataTypeInterface {
    String getDataType(String type);
}
