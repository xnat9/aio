package cn.xnatural.aio;

import cn.xnatural.enet.event.EL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.*;

/**
 * Aio Client
 */
public class AioClient extends AioBase {
    protected static final Logger                              log        = LoggerFactory.getLogger(AioClient.class);
    protected final        Map<String, SafeList<AioStream>>    sessionMap = new ConcurrentHashMap<>();
    protected final        AsynchronousChannelGroup            group;
    /**
     * 数据分割符(半包和粘包)
     */
    protected final byte[]                              delim;


    public AioClient(Map<String, Object> attrs, ExecutorService exec) {
        super(attrs, exec);
        try {
            String delimiter = getStr("delimiter", null);
            if (delimiter != null && !delimiter.isEmpty()) delim = delimiter.getBytes("utf-8");
            else delim = null;
            this.group = AsynchronousChannelGroup.withThreadPool(exec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    @EL(name = "sys.stopping", async = true)
    public void stop() {
        sessionMap.forEach((hp, ls) -> {
            for (Iterator<AioStream> itt = ls.iterator(); itt.hasNext(); ) {
                AioStream se = itt.next();
                itt.remove();
                se.close();
            }
        });
    }


    /**
     * 发送消息
     * @param host 主机名
     * @param port 端口
     * @param msgBytes 消息内容
     * @param failFn 失败回调
     * @param okFn 成功回调
     */
    public AioClient send(String host, Integer port, byte[] msgBytes, Consumer<Exception> failFn, Consumer<AioStream> okFn) {
        if (host == null || host.isEmpty()) throw new IllegalArgumentException("host must not be empty");
        if (port == null) throw new IllegalArgumentException("port must not be null");
        if (msgBytes == null || msgBytes.length < 1) throw new IllegalArgumentException("msgBytes must not be null");
        String key = host + ":" + port;
        final Supplier<AioStream> sessionSupplier = () -> {
            try {
                return getSession(host, port);
            } catch (Exception ex) { // 拿连接报错, 则回调失败函数
                if (failFn != null) failFn.accept(ex);
                else log.error("Send to " + key + " error. getSession", ex);
            }
            return null;
        };
        ByteBuffer msgBuf = ByteBuffer.allocate(msgBytes.length + (delim == null ? 0 : delim.length));
        msgBuf.put(msgBytes); if (delim != null) msgBuf.put(delim);

        final BiConsumer<Exception, AioStream> subFailFn = new BiConsumer<Exception, AioStream>() {
            @Override
            public void accept(Exception ex, AioStream session) {
                if (ex instanceof ClosedChannelException) { // 连接关闭时 重试
                    AioStream se = sessionSupplier.get();
                    if (se != null) {
                        se.write(msgBuf, this, (okFn == null ? null : () -> okFn.accept(se)));
                    }
                } else {
                    try {
                        log.error("Send to " + key + " error. " + session.channel.getLocalAddress() + " -> " + session.channel.getRemoteAddress(), ex);
                    } catch (IOException e) {
                        log.error("", e);
                    }
                }
            }
        };
        AioStream se = sessionSupplier.get();
        if (se != null) {
            se.write(msgBuf, subFailFn, (okFn == null ? null : () -> okFn.accept(se)));
        }
        return this;
    }


    public AioClient send(String host, Integer port, byte[] msgBytes) { return send(host, port, msgBytes, null, null); }


    protected void receive(byte[] bs, AioStream stream) {}


    /**
     * 获取AioSession
     * @param host
     * @param port
     * @return
     */
    protected AioStream getSession(String host, Integer port) {
        String key = host + ":" + port;
        AioStream session = null;
        SafeList<AioStream> ls = sessionMap.get(key);
        if (ls == null) {
            synchronized (sessionMap) {
                ls = sessionMap.get(key);
                if (ls == null) {
                    ls = new SafeList<>(); sessionMap.put(key, ls);
                    session = create(host, port); ls.add(session);
                }
            }
        }

        session = ls.findAny(se -> !se.busy());
        if (session == null) {
            session = create(host, port); ls.add(session);
        }

        return session;
    }


    /**
     * 创建 AioSession
     * @param host 目标主机
     * @param port 目标端口
     * @return {@link AioStream}
     */
    protected AioStream create(String host, Integer port) {
        String key = host + ":" + port;
        // 创建连接
        AsynchronousSocketChannel channel = null;
        try {
            channel = AsynchronousSocketChannel.open(group);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, getInteger("so_rcvbuf", 1024 * 1024 * 2));
            channel.setOption(StandardSocketOptions.SO_SNDBUF, getInteger("so_sndbuf", 1024 * 1024 * 2));
            channel.connect(new InetSocketAddress(host, port)).get(getLong("aioConnectTimeout", 3000L), TimeUnit.MILLISECONDS);
            log.info("New TCP(AIO) connection to " + key);
        } catch(Exception ex) {
            try {channel.close();} catch(Exception exx) {}
            throw new RuntimeException("连接错误. $key", ex);
        }
        AioStream se = new AioStream(channel, this) {
            @Override
            protected void doClose(AioStream session) {
                sessionMap.get(key).remove(session);
            }

            @Override
            protected void doRead(ByteBuffer bb) {
                if (delim == null) { // 没有分割符的时候
                    byte[] bs = new byte[buf.limit()];
                    buf.get(bs);
                    receive(bs, this);
                } else { // 分割 半包和粘包
                    do {
                        int delimIndex = indexOf(buf);
                        if (delimIndex < 0) break;
                        int readableLength = delimIndex - buf.position();
                        byte[] bs = new byte[readableLength];
                        buf.get(bs);
                        receive(bs, this);

                        // 跳过 分割符的长度
                        for (int i = 0; i < delim.length; i++) {buf.get();}
                    } while (true);
                    buf.compact();
                }
            }
        };
        se.start();
        return se;
    }


    /**
     * 查找分割符所匹配下标
     * @param buf
     * @return
     */
    protected int indexOf(ByteBuffer buf) {
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


    /**
     * 安全列表
     * @param <E>
     */
    public class SafeList<E> {
        protected final ArrayList<E>  data = new ArrayList<>();
        protected final ReadWriteLock lock = new ReentrantReadWriteLock();

        public E findAny(Function<E, Boolean> fn) {
            try {
                lock.readLock().lock();
                for (E e : data) {
                    if (fn.apply(e)) return e;
                }
            } finally {
                lock.readLock().unlock();
            }
            return null;
        }

        /**
         * 随机取一个 元素
         * @param predicate 符合条件的元素
         * @return
         */
        public E findRandom(Predicate<E> predicate) {
            try {
                lock.readLock().lock();
                if (data.isEmpty()) return null;
                if (predicate == null) {
                    return data.get(new Random().nextInt(data.size()));
                } else {
                    return data.stream().filter(predicate).findAny().orElse(null);
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        public void withWriteLock(Runnable fn) {
            if (fn == null) return;
            try {
                lock.writeLock().lock();
                fn.run();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void withReadLock(Runnable fn) {
            if (fn == null) return;
            try {
                lock.readLock().lock();
                fn.run();
            } finally {
                lock.readLock().unlock();
            }
        }

        public Iterator<E> iterator() { return data.iterator(); }

        public int size() { return data.size(); }

        public boolean isEmpty() { return data.isEmpty(); }

        public boolean contains(Object o) {
            try {
                lock.readLock().lock();
                return data.contains(o);
            } finally {
                lock.readLock().unlock();
            }
        }

        public boolean remove(Object o) {
            try {
                lock.writeLock().lock();
                return data.remove(o);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean add(E e) {
            try {
                lock.writeLock().lock();
                return data.add(e);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
