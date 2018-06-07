package cc.lfwzc.config;


import org.apache.log4j.Logger;

import java.io.*;

/**
 * 配置文件读取类
 * 用于读取配置文件内容并解析，生成配置内容对象
 */
public class ConfigFileReader {

    private static Logger logger = Logger.getLogger(ConfigFileReader.class.getName());

    public String readConfigFile(String filename) {
        InputStream configFileInputStream = null;
        try {
            configFileInputStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            logger.error("无法找到配置文件" + e.getLocalizedMessage());
            System.exit(-1);
        }
        BufferedReader fileReader = new BufferedReader(new InputStreamReader(configFileInputStream));
        String configJSONString = null;
        try {
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = fileReader.readLine()) != null) {
                sb.append(line + "/n");
            }
            fileReader.close();
            configFileInputStream.close();
            configJSONString = sb.toString();
            //去掉空格和换行
            configJSONString= configJSONString.replaceAll(" ","").replaceAll("/n", "");
        } catch (IOException e) {
            logger.error("读取配置文件错误" + e.getLocalizedMessage());
            System.exit(-1);
        }
        if (configJSONString == null) {
            logger.error("读取配置文件失败");
            System.exit(-1);
        }
        return configJSONString;
    }


}
