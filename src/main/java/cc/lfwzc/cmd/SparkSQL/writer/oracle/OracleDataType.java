package cc.lfwzc.cmd.SparkSQL.writer.oracle;

import cc.lfwzc.cmd.SparkSQL.writer.DataTypeInterface;

import java.util.HashMap;
import java.util.Map;

public class OracleDataType implements DataTypeInterface{
    private Map<String, String> typeMap;

    public OracleDataType() {
        typeMap = new HashMap<>();
        typeMap.put("String", "VARCHAR2(255)");
        typeMap.put("Date", "DATE");
        typeMap.put("Integer", "INTEGER");
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
