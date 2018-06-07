package cc.lfwzc.config.filter;

public class Action {
    private Integer ID = null;//要处理的列号
    private Filter filter = null;

    public Integer getID() {
        return ID;
    }

    public void setID(Integer ID) {
        this.ID = ID;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
