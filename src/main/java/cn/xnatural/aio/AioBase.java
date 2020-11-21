package cn.xnatural.aio;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * {@link AioClient}, {@link AioServer} 基类
 */
abstract class AioBase {
    protected final Map<String, Object> attrs;
    protected final ExecutorService     exec;


    public AioBase(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new HashMap<>() : attrs;
        this.exec = exec;
    }


    public void exec(Runnable fn) {
        if (exec == null) fn.run();
        else exec.execute(fn);
    }


    public Object attr(String key) {
        return attrs.get(key);
    }


    public Object attr(String key, Object value) {
        return attrs.put(key, value);
    }


    public String getStr(String key, String defaultValue) {
        Object r = attr(key);
        if (r == null) return defaultValue;
        return r.toString();
    }


    public Integer getInteger(String key, Integer defaultValue) {
        Object r = attr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Number) return ((Number) r).intValue();
        else return Integer.valueOf(r.toString());
    }


    public Long getLong(String key, Long defaultValue) {
        Object r = attr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Number) return ((Number) r).longValue();
        else return Long.valueOf(r.toString());
    }


    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object r = attr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Boolean) return ((Boolean) r);
        else return Boolean.valueOf(r.toString());
    }
}
