package cc.lfwzc.cmd.SparkStreaming.writer.oracle;

import cc.lfwzc.config.dst.DstConfig;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class OracleConnectionPool {
    private static Logger logger = Logger.getLogger(OracleConnectionPool.class.getName());
    private static LinkedList connectionQueue;

    public synchronized static Connection getConnection(DstConfig dstConfig) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            logger.error(e.getLocalizedMessage());
        }
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager
                            .getConnection("jdbc:oracle:thin:@localhost:1521:ORCL","C##HOSPITAL","123");
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return (Connection) connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
