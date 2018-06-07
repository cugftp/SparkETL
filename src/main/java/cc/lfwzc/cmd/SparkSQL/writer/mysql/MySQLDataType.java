package cc.lfwzc.cmd.SparkSQL.writer.mysql;

import cc.lfwzc.cmd.SparkSQL.writer.DataTypeInterface;

import java.util.HashMap;
import java.util.Map;

public class MySQLDataType implements DataTypeInterface {
    private Map<String, String> typeMap;

    public MySQLDataType() {
        typeMap = new HashMap<>();
        typeMap.put("String", "VARCHAR(255)");
        typeMap.put("Date", "DATETIME");
        typeMap.put("Integer", "INTEGER");
        typeMap.put("Long","BIGINT");
        typeMap.put("Float","FLOAT");
        typeMap.put("Double","DOUBLE");
    }

    @Override
    public String getDataType(String type) {
        if (typeMap.containsKey(type)) {
            return typeMap.get(type);
        }
        return null;
    }
}
