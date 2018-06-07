package cc.lfwzc.cmd.SparkStreaming;


import cc.lfwzc.config.ConfigFileReader;
import cc.lfwzc.config.dst.DstConfig;
import cc.lfwzc.config.dst.DstConfigParser;
import cc.lfwzc.config.filter.FilterConfig;
import cc.lfwzc.config.filter.FilterConfigParser;
import cc.lfwzc.config.src.SrcConfig;
import cc.lfwzc.config.src.SrcConfigParser;
import cc.lfwzc.filter.DoFilter;
import cc.lfwzc.filter.FilterResult;

import cc.lfwzc.cmd.SparkStreaming.writer.SQLWriterInterface;
import cc.lfwzc.cmd.SparkStreaming.writer.mysql.MySQLWriter;
import cc.lfwzc.cmd.SparkStreaming.writer.oracle.OracleWriter;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * 程序入口类
 * 使用SparkStreaming API接驳数据库进行数据清洗和抽取
 */
public class SparkStreamingETL {
    private static Logger logger = Logger.getLogger(SparkStreamingETL.class.getName());

    public static void main(String[] args) throws InterruptedException {

        //从传入参数读取配置文件路径
        if (args.length < 3) {
            logger.error("缺少配置文件路径参数");
            System.exit(-1);
        }
        //读取配置文件
        SrcConfig srcConfig = null;
        FilterConfig filterConfig = null;
        DstConfig dstConfig = null;
        try {
            String srcConfigFilename = args[0];
            String filterConfigFilename = args[1];
            String dstConfigFilename = args[2];

            String srcConfigString = new ConfigFileReader().readConfigFile(srcConfigFilename);
            String filterConfigString = new ConfigFileReader().readConfigFile(filterConfigFilename);
            String dstConfigString = new ConfigFileReader().readConfigFile(dstConfigFilename);

            srcConfig = new SrcConfigParser().parseConfig(srcConfigString);
            filterConfig = new FilterConfigParser().parseConfig(filterConfigString);
            dstConfig = new DstConfigParser().getDstConfig(dstConfigString);
        } catch (Exception e) {
            logger.error("读取配置文件错误" + e.getMessage());
            System.exit(-1);
        }
        if (srcConfig == null || filterConfig == null || dstConfig == null) {
            logger.error("读取配置文件错误");
            System.exit(-1);
        }

        //创建数据表
        SQLWriterInterface sqlWriter = null;
        switch (dstConfig.getDBType()) {
            case "mysql":
                sqlWriter = new MySQLWriter(dstConfig);
                break;
            case "oracle":
                sqlWriter = new OracleWriter(dstConfig);
                break;
            default:
                logger.error("不支持的数据库");
                System.exit(-1);
        }
        try {
            sqlWriter.createTable(dstConfig.getTableName(), dstConfig.getCols());
        } catch (Exception e) {
            logger.error("创建数据库遇到错误");
            System.exit(-1);
        }

        //直接启动需要设置SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("ETL01").setMaster("local");
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        JavaSparkContext sparkContext = new JavaSparkContext();

        //使用Spark Streaming连接Kafka
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(srcConfig.getBatchSize() * 1000));
        //配置kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", srcConfig.getKafka().getIp());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", srcConfig.getKafka().getGroupId());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = srcConfig.getKafka().getTopics();
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        //取得每行
        Gson gson = new Gson();
        final SrcConfig finalSrcConfig = srcConfig;
        final FilterConfig finalFilterConfig = filterConfig;
        //转成string才可以传递给map函数
        final String filterConfigString = gson.toJson(finalFilterConfig, FilterConfig.class);
        final String srcConfigString = gson.toJson(finalSrcConfig, SrcConfig.class);

        final DstConfig finalDstConfig = dstConfig;

        stream.map(new Function<ConsumerRecord<String, String>, Object>() {
            @Override
            public Object call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                //进行过滤操作

                //读取配置
                Gson gson = new Gson();
                FilterConfig filterConfig = gson.fromJson(filterConfigString, FilterConfig.class);
                SrcConfig srcConfig = gson.fromJson(srcConfigString, SrcConfig.class);

                String str = stringStringConsumerRecord.value();
                List<Map<Integer, String>> datas = new Vector<>();

//                logger.info("原始数据：" + str);
                Map<Integer, String> row = new HashMap<>();
                String[] rawData = str.split(srcConfig.getSplit());
                for (int i = 0; i < rawData.length; i++) {
                    row.put(i, rawData[i]);
                }

                //将原始的字符串数据放在最后
                row.put(rawData.length, str);
                datas.add(row);

                //进行过滤操作
                DoFilter doFilter = new DoFilter(filterConfig);
                FilterResult filterResult = doFilter.getFilterResult(datas);
                String filterResultString = gson.toJson(filterResult, FilterResult.class);
                return filterResultString;
            }
        }).foreachRDD(new VoidFunction<JavaRDD<Object>>() {
            @Override
            public void call(JavaRDD<Object> objectJavaRDD) throws Exception {
                SQLWriterInterface sqlWriter = null;
                switch (finalDstConfig.getDBType()) {
                    case "mysql":
                        sqlWriter = new MySQLWriter(finalDstConfig);
                        break;
                    case "oracle":
                        sqlWriter = new OracleWriter(finalDstConfig);
                        break;
                    default:
                        logger.error("不支持的数据库");
                        sparkContext.stop();
                }

                //获取结果
                List<Object> list = objectJavaRDD.collect();

                Gson gson = new Gson();
                for (Object filterResultString : list) {
                    FilterResult filterResult = gson.fromJson((String) filterResultString, FilterResult.class);
                    //将结果写入数据库
                    List<Map<Integer, String>> goodResults = filterResult.getGoodResults();
                    List<Map<Integer, String>> badResults = filterResult.getBadResults();
                    //写入好结果
                    for (Map<Integer, String> row : goodResults) {
                        Map<String, String> toWrite = new HashMap<>();
                        for (Integer colkey : row.keySet()) {
                            String name = finalDstConfig.getColumName(colkey);
                            if (name == null) {
                                continue;
                            }
                            toWrite.put(name, row.get(colkey));
                        }
                        if (toWrite.size() == 0) {
                            continue;
                        }
                        logger.info(toWrite);
                        sqlWriter.writeToTable(finalDstConfig.getTableName(), toWrite);
                    }
                    //写入坏结果
                    for (Map<Integer, String> row : badResults) {
                        //取得最后一列的原始字符串数据
                        Integer lastIndex = -1;
                        for (Integer key : row.keySet()) {
                            if (key > lastIndex) {
                                lastIndex = key;
                            }
                        }
                        String dirtyData = row.get(lastIndex);

                        logger.info("存在问题的数据：" + dirtyData);
                        sqlWriter.writeToDirtyTable(finalDstConfig.getTableName(), dirtyData);
                    }
                }
            }
        });


/*
        stream.map(new Function<ConsumerRecord<String, String>, Object>() {
            @Override
            public Object call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value();
            }
        }).foreachRDD(new VoidFunction<JavaRDD<Object>>() {
            @Override
            public void call(JavaRDD<Object> objectJavaRDD) throws Exception {
                SQLWriterInterface sqlWriter = null;
                switch (finalDstConfig.getDBType()) {
                    case "mysql":
                        sqlWriter = new MySQLWriter(finalDstConfig);
                        break;
                    case "oracle":
                        sqlWriter = new OracleWriter(finalDstConfig);
                        break;
                    default:
                        logger.error("不支持的数据库");
                        sparkContext.stop();
                }

                //对该行数据进行清洗，写入数据库

                List<Object> list = objectJavaRDD.collect();
                List<Map<Integer, String>> datas = new Vector<>();

                for (Object str : list) {
//                    logger.info("原始数据：" + (String) str);
                    Map<Integer, String> row = new HashMap<>();
                    String[] rawData = ((String) str).split(finalSrcConfig.getSplit());
                    for (int i = 0; i < rawData.length; i++) {
                        row.put(i, rawData[i]);
                    }

                    //将原始的字符串数据放在最后
                    row.put(rawData.length, (String) str);
                    datas.add(row);
                }

                //进行过滤操作
                DoFilter doFilter = new DoFilter(finalFilterConfig);
                FilterResult filterResult = doFilter.getFilterResult(datas);

                //将结果写入数据库
                List<Map<Integer, String>> goodResults = filterResult.getGoodResults();
                List<Map<Integer, String>> badResults = filterResult.getBadResults();
                //写入好结果
                for (Map<Integer, String> row : goodResults) {
                    Map<String, String> toWrite = new HashMap<>();
                    for (Integer colkey : row.keySet()) {
                        String name = finalDstConfig.getColumName(colkey);
                        if (name == null) {
                            continue;
                        }
                        toWrite.put(name, row.get(colkey));
                    }
                    if (toWrite.size() == 0) {
                        continue;
                    }
                    logger.info(toWrite);
                    sqlWriter.writeToTable(finalDstConfig.getTableName(), toWrite);
                }
                //写入坏结果
                for (Map<Integer, String> row : badResults) {
                    //取得最后一列的原始字符串数据
                    Integer lastIndex = -1;
                    for (Integer key : row.keySet()) {
                        if (key > lastIndex) {
                            lastIndex = key;
                        }
                    }
                    String dirtyData = row.get(lastIndex);

                    logger.info("存在问题的数据：" + dirtyData);
                    sqlWriter.writeToDirtyTable(finalDstConfig.getTableName(), dirtyData);
                }
            }
        });
*/
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }
}
