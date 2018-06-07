package cc.lfwzc.config.filter;


import com.google.gson.Gson;

/**
 * 由配置文件的JSON字符串生成对应的配置对象
 */
public class FilterConfigParser {
    public FilterConfig parseConfig(String configJSONString) {
        Gson gson = new Gson();
        FilterConfig filterConfig = gson.fromJson(configJSONString, FilterConfig.class);
        return filterConfig;
    }

}
