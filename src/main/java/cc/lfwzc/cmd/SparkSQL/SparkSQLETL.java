package cc.lfwzc.cmd.SparkSQL;

/**
 * 程序入口类
 * 使用SparkSQL API接驳数据库进行数据清洗和抽取
 */
import cc.lfwzc.config.ConfigFileReader;
import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.config.dst.DstConfigParser;
import cc.lfwzc.config.filter.FilterConfig;
import cc.lfwzc.config.filter.FilterConfigParser;
import cc.lfwzc.config.src.SqlSrcConfig;
import cc.lfwzc.config.src.SqlSrcConfigParser;
import cc.lfwzc.filter.DoFilter;
import cc.lfwzc.filter.FilterResult;
import cc.lfwzc.cmd.SparkSQL.writer.mysql.MySQLWriter;
import cc.lfwzc.cmd.SparkSQL.writer.SQLWriterInterface;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.In;

import java.util.*;

/**
 * 程序入口类
 * 使用SparkSQL API接驳数据库进行数据清洗和抽取
 */
public class SparkSQLETL
{
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkSQLETL.class);
    public static void main(String[] args)
    {
        //从传入参数读取配置文件路径
        if (args.length < 3)
        {
            logger.error("缺少配置文件路径参数");
            System.exit(-1);
        }
        String srcfilename="",dstfilename="",filterfilename="";
        srcfilename=args[0];
        filterfilename=args[1];
        dstfilename=args[2];
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf());
        SQLContext sqlContext = new SQLContext(sparkContext);
        //读取mysql数据
        Map<Integer,List<String>> result=readMySQL(sqlContext,srcfilename);
        //整理数据
        Vector<Map<Integer, String>>data=getdata(result);
        //清理和写数据
        dofilterandwrite(data,filterfilename,dstfilename);
        //停止SparkContext
        sparkContext.stop();
//        readOracle();

    }
    public static Map<Integer,List<String>> readMySQL(SQLContext sqlContext,String filename)
    {
        //根据配置文件生成类对象
        //"config-template/sparksqlsrcconfig-template.json"
        SqlSrcConfig a=new SqlSrcConfigParser().parseConfig(filename);
        //String url = "jdbc:mysql://120.78.168.177:3306/readcsvdata";
        String url=a.getUrl();
        //查找的表名
        String table=a.getReadtable();
        //String table = "copy1";
        //增加数据库的用户名(user)密码(password),指定数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", a.getUsername());
        connectionProperties.put("password", a.getPassword());
        connectionProperties.put("driver", a.getDriver());
        List<SqlSrcConfig.COLumns> coLumnsList=a.getColumn();
//        connectionProperties.put("user", "root");
//        connectionProperties.put("password", "123");
//        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");
        //SparkJdbc读取Postgresql的products表内容
        // 读取表中所有数据
        Map<Integer,List<String>>result=new HashMap<>();
       Dataset<Row> jdbcDF= sqlContext.read().jdbc(url,table,connectionProperties).select("*");
       for (int i=0;i<coLumnsList.size();i++)
       {
           Dataset<Row> j1 = jdbcDF.select(coLumnsList.get(i).getCol());
           List<Row> rowList = j1.collectAsList();
           List<String>templist=new ArrayList<>();

           for (Row row:rowList)
           {
               String str = row.toString();
               str=str.substring(1,str.length()-1);
               String[]temparray=str.split(",");
               for (String tempstr:temparray)
               {
                   templist.add(tempstr);
               }
           }
           result.put(coLumnsList.get(i).getId(),templist);
       }
        return result;
    }
    public static Vector<Map<Integer, String>> getdata(Map<Integer,List<String>>result)
    {
        Vector<Map<Integer, String>> data = new Vector<Map<Integer, String>>();
        Integer getonekey = null;
         for(Map.Entry<Integer,List<String>> entry : result.entrySet())
         {
                getonekey=entry.getKey();
                break;
         }
        for (int i=0;i<result.get(getonekey).size();i++)
        {
            Map<Integer, String> temprow = new HashMap<Integer, String>();
            for (Map.Entry<Integer, List<String>> entry : result.entrySet())
            {
                Integer key = entry.getKey();
                List<String> rowList = entry.getValue();
                temprow.put(key,rowList.get(i).toString());
            }
            data.add(temprow);
        }
        return data;
    }
    public static void  dofilterandwrite(Vector<Map<Integer, String>>data,String filterfilename,String dstfilename)
    {
        ConfigFileReader configFileReader = new ConfigFileReader();
        //"config-template/filterconfig-template.json"
        String jsonStr = configFileReader.readConfigFile(filterfilename);
        FilterConfigParser filterConfigParser = new FilterConfigParser();
        FilterConfig filterConfig = filterConfigParser.parseConfig(jsonStr);
        DoFilter doFilter = new DoFilter(filterConfig);
        FilterResult filterResult = doFilter.getFilterResult(data);
        Vector<Map<Integer, String>>goodresult=filterResult.getGoodResults();
        Vector<Map<Integer, String>>badresult=filterResult.getBadResults();

        ConfigFileReader conf=new ConfigFileReader();
        //"config-template/dstconfig-template.json"
        String str=conf.readConfigFile(dstfilename);
        DstConfigParser parser=new DstConfigParser();
        DstConfig dstConfig=parser.getDstConfig(str);
        SQLWriterInterface sqlWrite=new MySQLWriter(dstConfig);
        sqlWrite.createTable(dstConfig.getTableName(),dstConfig.getCols());
        for (int i=0;i<goodresult.size();i++)
        {
            Map<Integer,String> temp=goodresult.get(i);
            Map<String,String>colAndData=new HashMap<>();
            for (Map.Entry<Integer,String> entry : temp.entrySet())
            {
                String colname=dstConfig.getColumName(entry.getKey());
                String strs = entry.getValue();
                colAndData.put(colname,strs);
            }
            sqlWrite.writeToTable(dstConfig.getTableName(),colAndData);
        }
        for (int i=0;i<badresult.size();i++)
        {
            Map<Integer,String> temp=badresult.get(i);
            Map<String,String>colAndData=new HashMap<>();
            for (Map.Entry<Integer,String> entry : temp.entrySet())
            {
                String colname=dstConfig.getColumName(entry.getKey());
                String strs = entry.getValue();
                colAndData.put(colname,strs);
            }
            sqlWrite.writeToTable(dstConfig.getTableName()+"dirty",colAndData);
        }

    }
}
