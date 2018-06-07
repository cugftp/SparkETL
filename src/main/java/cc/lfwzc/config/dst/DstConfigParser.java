package cc.lfwzc.config.dst;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * 由配置文件的JSON字符串生成对应的配置对象
 */
public class DstConfigParser {
    private static Logger logger = Logger.getLogger(DstConfigParser.class.getName());

    public DstConfig getDstConfig(String configJSONString) {

        configJSONString=configJSONString.replaceAll("/n","");

        DstConfig dstConf=new DstConfig();

        Gson gson=new Gson();
        OutBean outBean=gson.fromJson(configJSONString,OutBean.class);
        InnerBean innerBean=outBean.getWriteTo();

        dstConf.SetDataBaseType(innerBean.getType());
        String url=null;
        if(dstConf.getDBType().equals("mysql")){
            url="jdbc:mysql://%s:%d/%s?characterEncoding=%s&useSSL=true";
            url=String.format(url,innerBean.getIp(),innerBean.getPort(),innerBean.getDatabasename(),
                    innerBean.getCharacterEncoding());
        }else if(dstConf.getDBType().equals("oracle")){
            url="jdbc:oracle:thin:@%s:%d:%s";
            url=String.format(url,innerBean.getIp(),innerBean.getPort(),innerBean.getDatabasename());
        }
        dstConf.SetURL(url);
        dstConf.setCharset(innerBean.getCharacterEncoding());
        dstConf.SetUesr(innerBean.getUsername());
        dstConf.SetPassword(innerBean.getPassword());
        dstConf.SetTableName(innerBean.getTablename());
        for(InnerBean.Cols col:innerBean.getCols()){
            dstConf.addColum(col.getName(),col.getType());
            dstConf.addColumName(col.getColId(),col.getName());
        }
        return dstConf;
    }

}
