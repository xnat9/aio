package cn.xnatural.aio;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * {@link AioClient}, {@link AioServer} 基类
 */
abstract class AioBase {
    /**
     * 属性集
     */
    protected final Map<String, Object> attrs;
    /**
     * 执行线程池
     */
    protected final ExecutorService     exec;


    public AioBase(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new HashMap<>() : attrs;
        this.exec = exec;
    }


    public void exec(Runnable fn) {
        if (exec == null) fn.run();
        else exec.execute(fn);
    }


    /**
     * 查找分割符所匹配下标
     * @param buf
     * @param delim 分隔符
     * @return 下标位置
     */
    protected int indexOf(ByteBuffer buf, byte[] delim) {
        byte[] hb = buf.array();
        int delimIndex = -1; // 分割符所在的下标
        for (int i = buf.position(), size = buf.limit(); i < size; i++) {
            boolean match = true; // 是否找到和 delim 相同的字节串
            for (int j = 0; j < delim.length; j++) {
                match = match && (i + j < size) && delim[j] == hb[i + j];
            }
            if (match) {
                delimIndex = i;
                break;
            }
        }
        return delimIndex;
    }


    public Object getAttr(String key) { return attrs.get(key); }


    public Object setAttr(String key, Object value) { return attrs.put(key, value); }


    public String getStr(String key, String defaultValue) {
        Object r = getAttr(key);
        if (r == null) return defaultValue;
        return r.toString();
    }


    public Integer getInteger(String key, Integer defaultValue) {
        Object r = getAttr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Number) return ((Number) r).intValue();
        else return Integer.valueOf(r.toString());
    }


    public Long getLong(String key, Long defaultValue) {
        Object r = getAttr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Number) return ((Number) r).longValue();
        else return Long.valueOf(r.toString());
    }


    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object r = getAttr(key);
        if (r == null) return defaultValue;
        else if (r instanceof Boolean) return ((Boolean) r);
        else return Boolean.valueOf(r.toString());
    }
}
