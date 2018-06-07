package cc.lfwzc.config.dst;

import java.util.HashMap;
import java.util.Map;

/**
 * 目标数据库配置类
 */
public class DstConfig {
    private String DBType;
    private String URL;
    private String user;
    private String password;
    private String tableName;
    private String charset;
    private Map<String,String> cols;
    private Map<Integer,String> idAndName;
    public DstConfig(){
        cols=new HashMap<>();
        idAndName=new HashMap<>();
    }

    public void setCharset(String charset){
        this.charset=charset;
    }

    public String getCharset(){
        return charset;
    }

    public void SetDataBaseType(String dbType)
    {
        DBType=dbType;
    }

    public void SetURL(String url)
    {
        URL=url;
    }

    public void SetUesr(String uesr)
    {
        this.user=uesr;
    }

    public void SetPassword(String password)
    {
        this.password=password;
    }

    public void SetTableName(String tableName)
    {
        this.tableName=tableName;
    }

    public void addColum(String Name,String Type)
    {
        cols.put(Name,Type);
    }

    public void addColumName(Integer colId,String name){
        idAndName.put(colId,name);
    }

    public String getDBType()
    {
        return DBType;
    }

    public String getURL()
    {
        return URL;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public String getTableName()
    {
        return tableName;
    }

    public Map<String ,String> getCols()
    {
        return cols;
    }

    public Map<Integer,String> getIdAndName(){
        return idAndName;
    }

    public String getColumType(String colName){
        if (cols.containsKey(colName)) {
            return cols.get(colName);
        }
        return null;
    }

    public String getColumName(Integer id){
        if (idAndName.containsKey(id)) {
            return idAndName.get(id);
        }
        return null;
    }


}
