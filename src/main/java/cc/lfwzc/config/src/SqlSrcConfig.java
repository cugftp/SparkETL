package cc.lfwzc.config.src;


import java.util.*;

/**
 * 数据源配置类
 * 配置对象类的声明
 */
public class SqlSrcConfig
{
    private String type;
    private String ip;
    private String port;
    private String database;
    private String user;
    private String password;
    private String table;
    private List<COLumns>cols;
    public static class COLumns
    {
        private Integer id;
        private String col;

        public void setId(Integer id)
        {
            this.id = id;
        }

        public Integer getId()
        {
            return id;
        }

        public void setCol(String col)
        {
            this.col = col;
        }

        public String getCol()
        {
            return col;
        }
    }
    public String getType()
    {
        return type;
    }
    public String getIp(){return ip;}
    public String getPort(){return port;}
    public String getDatabase(){return database;}
    public String getUrl()
    {
        String url="";
        if (type.equals("mysql"))
        {
            url="jdbc:mysql://"+ip+":"+port+"/"+database;
        }
        if (type.equals("oracle"))
        {
            url="jdbc:oracle:thin:"+user+"/"+password+"@//"+ip+":"+port+"/"+database;
        }
        return url;
    }
    public String getUsername()
    {
        return user;
    }
    public String getPassword()
    {
        return password;
    }
    public String getReadtable()
    {
        return table;
    }
    public String getDriver()
    {
        String driver="";
        if(type.equals("mysql"))
        {
            driver="com.mysql.cj.jdbc.Driver";
        }
        if (type.equals("oracle"))
        {
            driver="oracle.jdbc.driver.OracleDriver";
        }
        return driver;
    }
    public void setColumns(List<COLumns> cols)
    {
        this.cols = cols;
    }
    public List<COLumns> getColumn()
    {
        return cols;
    }
}

