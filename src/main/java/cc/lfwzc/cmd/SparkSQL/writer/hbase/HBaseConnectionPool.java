package cc.lfwzc.cmd.SparkSQL.writer.hbase;

import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.cmd.SparkSQL.writer.mysql.MySQLConnectionPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.DriverManager;
import java.util.LinkedList;

public class HBaseConnectionPool {
    private static Logger logger = Logger.getLogger(HBaseConnectionPool.class.getName());
    private static LinkedList connectionQueue;

    public synchronized static Connection getConnection(DstConfig dstConfig) {
        try {
            if (connectionQueue == null) {
                Configuration conf= HBaseConfiguration.create();
                //设置Zookeeper,直接设置IP地址
                conf.set("hbase.zookeeper.property.clientPort","2181");
                conf.set("hbase.zookeeper.quorum","localhost");
                connectionQueue = new LinkedList();
                for (int i = 0; i < 5; i++) {
                    Connection conn = ConnectionFactory.createConnection(conf);
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
        return (Connection) connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
