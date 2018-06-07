package cc.lfwzc.config.src;


import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * 由配置文件的JSON字符串生成对应的配置对象
 */
public class SrcConfigParser {
    private static Logger logger = Logger.getLogger(SrcConfigParser.class.getName());

    public SrcConfig parseConfig(String configJSONString) {
        Gson gson = new Gson();
        SrcConfig srcConfig = gson.fromJson(configJSONString, SrcConfig.class);
        return srcConfig;
    }
}
