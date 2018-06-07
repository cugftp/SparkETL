
package cc.lfwzc.config.src;

import cc.lfwzc.config.ConfigFileReader;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.log4j.Logger;

/**
 * 由配置文件的JSON字符串生成对应的配置对象
 */
public class SqlSrcConfigParser
{
    private static Logger logger = Logger.getLogger(SqlSrcConfigParser.class.getName());
    public SqlSrcConfig parseConfig(String filename)
    {
        SqlSrcConfig config = null;
        try
        {
            String configJSONString=new ConfigFileReader().readConfigFile(filename);
            Gson gson = new Gson();
            config = gson.fromJson(configJSONString, SqlSrcConfig.class);
        } catch (JsonSyntaxException e) {
            logger.error(e.getLocalizedMessage());
            System.exit(-1);
        }
        return config;
    }
}