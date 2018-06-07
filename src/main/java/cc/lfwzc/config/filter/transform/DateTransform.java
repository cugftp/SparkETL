package cc.lfwzc.config.filter.transform;

public class DateTransform {
    private String inputDateFormat = null;//输入的日期格式，如“yyyy-MM-DD”
    private String outputDateFormat = null;//输出的日期格式，如“MM-DD-yyyy”

    public String getInputDateFormat() {
        return inputDateFormat;
    }

    public void setInputDateFormat(String inputDateFormat) {
        this.inputDateFormat = inputDateFormat;
    }

    public String getOutputDateFormat() {
        return outputDateFormat;
    }

    public void setOutputDateFormat(String outputDateFormat) {
        this.outputDateFormat = outputDateFormat;
    }
}
