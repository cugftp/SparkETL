package cc.lfwzc.cmd.SparkSQL.writer.mysql;

import cc.lfwzc.config.dst.DstConfig;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class MySQLConnectionPool {
    private static Logger logger = Logger.getLogger(MySQLConnectionPool.class.getName());
    private static LinkedList connectionQueue;

    public synchronized static Connection getConnection(DstConfig dstConfig) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error(e.getLocalizedMessage());
        }
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager
                            .getConnection(dstConfig.getURL(),dstConfig.getUser(),dstConfig.getPassword());
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
