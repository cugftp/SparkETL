package cc.lfwzc.filter;

import java.util.Map;
import java.util.Vector;

/**
 * 过滤结果类
 */
public class FilterResult {
    private Vector<Map<Integer, String>> goodResults;
    private Vector<Map<Integer, String>> badResults;

    public Vector<Map<Integer, String>> getGoodResults() {
        return goodResults;
    }

    public void setGoodResults(Vector<Map<Integer, String>> goodResults) {
        this.goodResults = goodResults;
    }

    public Vector<Map<Integer, String>> getBadResults() {
        return badResults;
    }

    public void setBadResults(Vector<Map<Integer, String>> badResults) {
        this.badResults = badResults;
    }
}
