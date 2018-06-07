package cc.lfwzc.cmd.SparkSQL.writer.hbase;

import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.cmd.SparkSQL.writer.SQLWriterInterface;
import cc.lfwzc.cmd.SparkSQL.writer.hbase.HBaseConnectionPool;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HBaseWriter implements SQLWriterInterface{
    private static Logger logger = Logger.getLogger(HBaseWriter.class.getName());
    private DstConfig dstConfig;

    public HBaseWriter(DstConfig dstConfig){
        this.dstConfig=dstConfig;
    }

    @Override
    public boolean createTable(String tablename, Map<String, String> cols){
        Connection connection=HBaseConnectionPool.getConnection(dstConfig);
        try {
            Admin admin=connection.getAdmin();
            //检查表是否存在，存在，则删除表
            TableName tableName=TableName.valueOf(tablename);
            if(admin.tableExists(tableName)){
                deleteTable(admin,tableName);
            }
            TableName dirtyTableName=TableName.valueOf(tablename+"dirty");
            if(admin.tableExists(dirtyTableName)){
                deleteTable(admin,dirtyTableName);
            }
            //创建表
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for( String colName:cols.keySet()) {
                tableDescriptor.addFamily(new HColumnDescriptor(colName));
            }
            admin.createTable((TableDescriptor) tableDescriptor);

            HTableDescriptor dirtyTableDescriptor = new HTableDescriptor(tableName);
            dirtyTableDescriptor.addFamily(new HColumnDescriptor("processtime"));
            dirtyTableDescriptor.addFamily(new HColumnDescriptor("dirtydata"));
            admin.createTable((TableDescriptor) dirtyTableDescriptor);
            HBaseConnectionPool.returnConnection(connection);
        }catch (MasterNotRunningException e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            return false;
        } catch (ZooKeeperConnectionException e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void deleteTable(Admin admin,TableName tableName){
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }


    @Override
    public void writeToTable(String tablename , Map<String,String> colAndData){

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
