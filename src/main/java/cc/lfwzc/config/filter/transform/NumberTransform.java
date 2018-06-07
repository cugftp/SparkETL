package cc.lfwzc.config.filter.transform;

public class NumberTransform {
    private Double add = null;// +=
    private Double sub = null;// -=
    private Double mul = null;// *=
    private Double div = null;// /=
    private Integer precision = null;//保留的小数位数，四舍五入

    public Double getAdd() {
        return add;
    }

    public void setAdd(Double add) {
        this.add = add;
    }

    public Double getSub() {
        return sub;
    }

    public void setSub(Double sub) {
        this.sub = sub;
    }

    public Double getMul() {
        return mul;
    }

    public void setMul(Double mul) {
        this.mul = mul;
    }

    public Double getDiv() {
        return div;
    }

    public void setDiv(Double div) {
        this.div = div;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }
}
