package cc.lfwzc.config.dst;

import java.util.List;

public class InnerBean {
    private String type;
    private String ip;
    private int port;
    private String databasename;
    private String characterEncoding;
    private String username;
    private String password;
    private String tablename;
    private List<Cols> cols;

    public void setType(String type){
        this.type=type;
    }

    public String getType(){
        return type;
    }

    public void setIp(String ip){
        this.ip=ip;
    }

    public String getIp()
    {
        return ip;
    }

    public void setPort(int port){
        this.port=port;
    }

    public int getPort(){
        return port;
    }

    public void setDatabasename(String databasename){
        this.databasename=databasename;
    }

    public String getDatabasename(){
        return databasename;
    }

    public void setCharacterEncoding(String characterEncoding){
        this.characterEncoding=characterEncoding;
    }

    public String getCharacterEncoding(){
        return characterEncoding;
    }

    public void setUsername(String username){
        this.username=username;
    }

    public String getUsername(){
        return username;
    }

    public void setPassword(String password){
        this.password=password;
    }

    public String getPassword(){
        return password;
    }

    public void setTablename(String tablename){
        this.tablename=tablename;
    }

    public String getTablename(){
        return tablename;
    }

    public void setCols(List<Cols> cols){
        this.cols=cols;
    }

    public List<Cols> getCols(){
        return cols;
    }

    public static class Cols{
        private String name;
        private String type;
        private Integer colId;

        public void setName(String name){
            this.name=name;
        }

        public String getName(){
            return name;
        }

        public void setType(String type){
            this.type=type;
        }

        public String getType(){
            return type;
        }

        public void setColId(Integer colID){
            this.colId=colID;
        }

        public Integer getColId(){
            return colId;
        }
    }

}
