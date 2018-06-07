package cc.lfwzc.cmd.SparkStreaming.writer.oracle;

import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.cmd.SparkStreaming.writer.DataTypeInterface;
import cc.lfwzc.cmd.SparkStreaming.writer.SQLWriterInterface;
import cc.lfwzc.cmd.SparkStreaming.writer.oracle.OracleDataType;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class OracleWriter implements SQLWriterInterface {
    private static Logger logger = Logger.getLogger(OracleWriter.class.getName());
    private final static String dropTableQuery = "BEGIN" +
            "    EXECUTE IMMEDIATE 'DROP TABLE ?';" +
            "    EXCEPTION WHEN OTHERS THEN NULL;" +
            "END;";

    private final static String createDirtyTableQuery =
            "CREATE TABLE ? (processtime DATE,dirtydata VARCHAR2(255))";

    private DstConfig dstConfig;

    public OracleWriter(DstConfig dstConfig){
        this.dstConfig=dstConfig;
    }

    @Override
    public boolean createTable(String tablename, Map<String, String> cols) {
        Connection connection=OracleConnectionPool.getConnection(dstConfig);
        try{
            //删除已经存在的表
            connection.setAutoCommit(false);
            Statement statement0 =
                    connection.createStatement();
            statement0.execute(dropTableQuery.replaceAll("\\?", tablename));
            statement0.close();
            PreparedStatement preparedStatement =
                    connection.prepareStatement(dropTableQuery);
            Statement statement1 =
                    connection.createStatement();
            statement1.execute(dropTableQuery.replaceAll("\\?", tablename+"dirty"));
            statement1.close();


            OracleDataType oracleDataType = new OracleDataType();
            StringBuilder createTableSQL = new StringBuilder("CREATE TABLE ");
            createTableSQL.append(tablename+" (");
            for (String colName : cols.keySet()) {
                createTableSQL.append(colName
                        + " "
                        + oracleDataType.getDataType(dstConfig.getColumType(colName))
                        + ",");
            }
            createTableSQL.deleteCharAt(createTableSQL.length() - 1);
            createTableSQL.append(")");
            Statement statement = connection.createStatement();
            statement.execute(createTableSQL.toString());
            statement.execute(
                    createDirtyTableQuery
                            .replace("?", tablename + "dirty"));
            statement.close();
            connection.commit();
            OracleConnectionPool.returnConnection(connection);

        }catch (SQLException e){
            logger.error(e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @Override
    public void writeToTable(String tablename , Map<String,String> colAndData) {
        Connection connection = OracleConnectionPool.getConnection(dstConfig);
        DataTypeInterface oracleDataType=new OracleDataType();
        //插入语句
        String insertSQL="INSERT INTO %s ( %s ) VALUES ( %s )";
        String colsName="",colsValues="";
        for(String colName:colAndData.keySet()){
            colsName+=colName+",";
            colsValues+="'"+colAndData.get(colName)+"',";
        }
        colsName=colsName.substring(0,colsName.length()-1);
        colsValues=colsValues.substring(0,colsValues.length()-1);
        insertSQL=String.format(insertSQL, tablename,colsName,colsValues);
        System.out.println(insertSQL);
        //执行插入语句
        try {
            Statement statement = connection.createStatement();
            statement.execute(insertSQL);
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage());
        }
        OracleConnectionPool.returnConnection(connection);
    }

    @Override
    public void writeToDirtyTable(String tablename,String dirtyData){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String,String> dirty=new HashMap<>();
        dirty.put("processtime",df.format(new Date()).toString());
        dirty.put("dirtydata",dirtyData);
        writeToTable(tablename+"dirty",dirty);
    }
}
