package cc.lfwzc.cmd.SparkSQL.writer.mysql;

import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.cmd.SparkSQL.writer.DataTypeInterface;
import cc.lfwzc.cmd.SparkSQL.writer.SQLWriterInterface;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MySQLWriter implements SQLWriterInterface {
    private static Logger logger = Logger.getLogger(MySQLWriter.class.getName());

    private final static String dropTableQuery = "DROP TABLE IF EXISTS ?;";
    /*private final static String createDirtyTableQuery =
            "CREATE TABLE ? (processtime DATETIME,dirtydata VARCHAR(255));";*/
    private DstConfig dstConfig;

    public MySQLWriter(DstConfig dstConfig) {
        this.dstConfig=dstConfig;
    }

    @Override
    public boolean createTable(String tablename, Map<String, String> cols) {
        Connection connection = MySQLConnectionPool.getConnection(dstConfig);
        try {
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

            MySQLDataType mySQLDataType = new MySQLDataType();
            StringBuilder createGoodTableSQL = new StringBuilder("CREATE TABLE ");
            createGoodTableSQL.append("`"+tablename + "` (");
            for (String colName : cols.keySet()) {
                createGoodTableSQL.append(colName
                        + " "
                        + mySQLDataType.getDataType(dstConfig.getColumType(colName))
                        + ",");
            }
            createGoodTableSQL.deleteCharAt(createGoodTableSQL.length() - 1);
            createGoodTableSQL.append(")charset="+dstConfig.getCharset()+";");
            Statement statement = connection.createStatement();
            statement.execute(createGoodTableSQL.toString());

            StringBuilder createBadTableSQL = new StringBuilder("CREATE TABLE ");
            createBadTableSQL.append("`"+tablename + "dirty` (");
            for (String colName : cols.keySet()) {
                createBadTableSQL.append(colName
                        + " "
                        + "VARCHAR(255)"
                        + ",");
            }
            createBadTableSQL.deleteCharAt(createBadTableSQL.length() - 1);
            createBadTableSQL.append(")charset="+dstConfig.getCharset()+";");
            statement.execute(createBadTableSQL.toString());
            statement.close();
            connection.commit();
            MySQLConnectionPool.returnConnection(connection);
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @Override
    public void writeToTable(String tablename, Map<String, String> colAndData) {
        Connection connection = MySQLConnectionPool.getConnection(dstConfig);
        DataTypeInterface mySQLDataType=new MySQLDataType();
        //插入语句
        String insertSQL="INSERT INTO %s ( %s ) VALUES ( %s );";
        String colsName="",colsValues="";
        for(String colName:colAndData.keySet()){
            colsName+=colName+",";
            colsValues+="'"+colAndData.get(colName)+"',";

        }
        colsName=colsName.substring(0,colsName.length()-1);
        colsValues=colsValues.substring(0,colsValues.length()-1);
        insertSQL=String.format(insertSQL, tablename,colsName,colsValues);
        //执行插入语句
        try {
            Statement statement = connection.createStatement();
            statement.execute(insertSQL);
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage());
        }
        MySQLConnectionPool.returnConnection(connection);
    }
    @Override
    public void writeToDirtyTable(String tablename, String dirtyData) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, String> dirty = new HashMap<>();
        dirty.put("processtime", df.format(new Date()).toString());
        dirty.put("dirtydata", dirtyData);
        writeToTable(tablename + "dirty", dirty);
    }
}
