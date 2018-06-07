package cc.lfwzc.config.dst;

public class OutBean {
    private InnerBean writeTo;

    public void setWriteTo(InnerBean writeTo){
        this.writeTo=writeTo;
    }

    public InnerBean getWriteTo(){
        return writeTo;
    }

}
