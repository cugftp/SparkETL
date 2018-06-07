package cc.lfwzc.config.filter;

import cc.lfwzc.config.filter.transform.DateTransform;
import cc.lfwzc.config.filter.transform.NumberTransform;
import cc.lfwzc.config.filter.transform.StringTransform;

public class Filter {
    private Boolean nullable = null;
    private String regexp = null;
    private Double min = null;//针对数值类型的列，最小值
    private Double max = null;//针对数值类型的列，最大值
    private DateTransform transdate = null;//针对日期类型，提供简单的格式转换
    private NumberTransform transnumber = null;//针对数值类型，提供简单加减乘除
    private StringTransform transstring = null;//针对字符串类型，提供简单的字符串转换

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getRegexp() {
        return regexp;
    }

    public void setRegexp(String regexp) {
        this.regexp = regexp;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public DateTransform getTransdate() {
        return transdate;
    }

    public void setTransdate(DateTransform transdate) {
        this.transdate = transdate;
    }

    public NumberTransform getTransnumber() {
        return transnumber;
    }

    public void setTransnumber(NumberTransform transnumber) {
        this.transnumber = transnumber;
    }

    public StringTransform getTransstring() {
        return transstring;
    }

    public void setTransstring(StringTransform transstring) {
        this.transstring = transstring;
    }
}
