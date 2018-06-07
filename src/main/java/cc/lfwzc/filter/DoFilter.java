package cc.lfwzc.filter;

import cc.lfwzc.config.filter.Action;
import cc.lfwzc.config.filter.Filter;
import cc.lfwzc.config.filter.FilterConfig;
import cc.lfwzc.config.filter.transform.DateTransform;
import cc.lfwzc.config.filter.transform.NumberTransform;
import org.apache.log4j.Logger;
import scala.Int;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 过滤器类
 */
public class DoFilter {
    private final static Logger logger = Logger.getLogger(DoFilter.class.getName());

    private FilterConfig filterConfig = null;

    public DoFilter(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    public FilterResult getFilterResult(List<Map<Integer, String>> data) {
        FilterResult filterResult = new FilterResult();
        //存放过滤通过的结果
        Vector<Map<Integer, String>> goodResults = new Vector<>();
        //存放无法通过的结果
        Vector<Map<Integer, String>> badResults = new Vector<>();


        for (int i = 0; i < data.size(); i++) {
            //获取每一行
            Map<Integer, String> row = data.get(i);
            //复制一份
            Map<Integer, String> copy = new HashMap<>();
            for (Map.Entry<Integer, String> col : row.entrySet()) {
                copy.put(col.getKey(), col.getValue());
            }
            boolean isBad = false;

            //执行配置文件中的每一个过滤操作
            for (int j = 0; j < filterConfig.getActions().size(); j++) {
                //获取要执行的操作
                Action action = filterConfig.getActions().get(j);
                //获取要操作的列
                if (row.containsKey(action.getID())) {
                    String colData = row.get(action.getID());
                    //根据action中定义的过滤和转换规则进行操作
                    Filter filter = action.getFilter();
                    //是否符合正则表达式
                    if (filter.getRegexp() != null) {
                        if (!Pattern.matches(filter.getRegexp(), colData)) {
                            isBad = true;
                            break;
                        }
                    }
                    //检查是否符合数字的过滤要求
                    if (filter.getMin() != null ||
                            filter.getMax() != null ||
                            filter.getTransnumber() != null) {
                        String res = numberFilterAndTransform(
                                colData, filter.getMin(), filter.getMax(),
                                filter.getTransnumber());
                        if (res != null) {
                            row.remove(action.getID());
                            row.put(action.getID(), res);
                            continue;
                        } else {
                            isBad = true;
                            break;
                        }
                    }
                    //检查是否符合日期的要求
                    if (filter.getTransdate() != null) {
                        String res = dateFilterAndTransform(colData, filter.getTransdate());
                        if (res != null) {
                            row.remove(action.getID());
                            row.put(action.getID(), res);
                        } else {
                            isBad = true;
                            break;
                        }
                    }
                }
            }
            if (isBad) {
                badResults.add(copy);
            } else {
                goodResults.add(row);
            }
        }
        filterResult.setGoodResults(goodResults);
        filterResult.setBadResults(badResults);
        return filterResult;
    }

    //对数字进行过滤，如果未通过则返回null，如果定义了转换则返回转换后的数字
    private String numberFilterAndTransform(String data, Double min, Double max, NumberTransform numberTransform) {
        Double number = null;
        try {//解析数字
            number = Double.valueOf(data);
        } catch (NumberFormatException e) {
            return null;
        }
        //判断是否超出范围
        if (min != null && number < min) {
            return null;
        }
        if (max != null && number > max) {
            return null;
        }
        if (numberTransform != null) {
            if (numberTransform.getAdd() != null) {
                number += numberTransform.getAdd();
            }
            if (numberTransform.getSub() != null) {
                number -= numberTransform.getSub();
            }
            if (numberTransform.getMul() != null) {
                number *= numberTransform.getMul();
            }
            if (numberTransform.getDiv() != null) {
                if (numberTransform.getDiv() == 0) {
                    return null;
                } else {
                    number /= numberTransform.getDiv();
                }
            }
            if (numberTransform.getPrecision() != null &&
                    numberTransform.getPrecision() >= 0) {
                BigDecimal bg = new BigDecimal(number);
                number = bg.setScale(numberTransform.getPrecision(),
                        BigDecimal.ROUND_HALF_UP)
                        .doubleValue();
            }
        }
        return number.toString();
    }

    //对日期字符串进行解析，如果出错或解析失败则返回null，如果定义了输出格式则按输出格式返回新字符串
    private String dateFilterAndTransform(String data, DateTransform dateTransform) {
        if (dateTransform != null) {
            Date date = null;
            if (dateTransform.getInputDateFormat() != null) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                        dateTransform.getInputDateFormat());
                try {
                    date = simpleDateFormat.parse(data);
                } catch (ParseException e) {
                    return null;
                }
            }
            if (dateTransform.getOutputDateFormat() != null) {
                if (date != null) {
                    try {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                                dateTransform.getOutputDateFormat());
                        return simpleDateFormat.format(date);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                }
            }
        }
        return null;
    }

}