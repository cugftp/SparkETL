package cc.lfwzc.config.filter;

import java.util.Vector;

/**
 * 过滤器配置类
 */
public class FilterConfig {
    private Vector<Action> actions = null;

    public Vector<Action> getActions() {
        return actions;
    }

    public void setActions(Vector<Action> actions) {
        this.actions = actions;
    }
}
