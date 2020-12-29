package cn.xnatural.aio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Aio Client
 */
public class AioClient extends AioBase {
    protected static final Logger                              log        = LoggerFactory.getLogger(AioClient.class);
    /**
     * host:port -> List<AioStream>
     */
    protected final        Map<String, SafeList<AioStream>> streamMap = new ConcurrentHashMap<>();
    /**
     * {@link AsynchronousChannelGroup}
     */
    protected final        AsynchronousChannelGroup            group;
    /**
     * 数据分割符(半包和粘包)
     */
    protected final byte[]                              delim;


    /**
     * 创建 {@link AioClient}
     * @param attrs 属性集
     *              delimiter: 分隔符
     *              maxMsgSize: socket 每次取数据的最大
     *              writeTimeout: 数据写入超时时间. 单位:毫秒
     *              connectTimeout: 连接超时时间. 单位:毫秒
     * @param exec 线程池
     */
    public AioClient(Map<String, Object> attrs, ExecutorService exec) {
        super(attrs, exec);
        try {
            String delimiter = getStr("delimiter", null);
            if (delimiter != null && !delimiter.isEmpty()) delim = toByte(delimiter, getStr("charset", "utf-8"));
            else delim = null;
            attrs.put("delim", delim);
            this.group = AsynchronousChannelGroup.withThreadPool(exec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 停止,清除并关闭连接
     */
    public void stop() {
        if (group.isShutdown()) return;
        group.shutdown();
        streamMap.forEach((hp, ls) -> {
            ls.withWriteLock(() -> {
                for (Iterator<AioStream> itt = ls.iterator(); itt.hasNext(); ) {
                    AioStream se = itt.next();
                    itt.remove(); se.close();
                }
            });
        });
    }


    /**
     * 发送消息
     * @param host 主机名/ip
     * @param port 端口
     * @param msgBytes 消息内容
     * @param failFn 失败回调
     * @param okFn 成功回调
     * @return {@link AioClient}
     */
    public AioClient send(String host, Integer port, byte[] msgBytes, Consumer<Exception> failFn, Consumer<AioStream> okFn) {
        if (group.isShutdown()) return this;
        if (host == null || host.isEmpty()) throw new IllegalArgumentException("Param host not empty");
        if (port == null) throw new IllegalArgumentException("Param port required");
        if (msgBytes == null || msgBytes.length < 1) throw new IllegalArgumentException("Param msgBytes not empty");
        String key = host + ":" + port;
        final Supplier<AioStream> streamSupplier = () -> { // aioStream 获取函数
            try {
                return getSession(host, port);
            } catch (Exception ex) { // 拿连接报错, 则回调失败函数
                if (failFn != null) failFn.accept(ex);
                else log.error("Send to " + key + " error. getStream", ex);
            }
            return null;
        };
        // 把发送在数据组装成 ByteBuffer
        ByteBuffer msgBuf = ByteBuffer.allocate(msgBytes.length + (delim == null ? 0 : delim.length));
        msgBuf.put(msgBytes); if (delim != null) msgBuf.put(delim); msgBuf.flip();
        final BiConsumer<Exception, AioStream> subFailFn = new BiConsumer<Exception, AioStream>() { // AioStream 写入流错误的回调函数
            @Override
            public void accept(Exception ex, AioStream session) {
                if (ex instanceof ClosedChannelException) { // 连接关闭时 重试
                    AioStream se = streamSupplier.get();
                    if (se != null) {
                        se.write(msgBuf, this, (okFn == null ? null : () -> okFn.accept(se)));
                    }
                } else {
                    if (failFn == null) log.error("Write to " + key + " error. " + session.toString(), ex);
                    else failFn.accept(ex);
                }
            }
        };
        AioStream se = streamSupplier.get();
        if (se != null) {
            se.write(msgBuf, subFailFn, (okFn == null ? null : () -> okFn.accept(se)));
        }
        return this;
    }


    /**
     * {@link #send(String, Integer, byte[], Consumer, Consumer)}
     * @param host 主机名/ip
     * @param port 端口
     * @param msgBytes 消息字节
     * @return {@link AioClient}
     */
    public AioClient send(String host, Integer port, byte[] msgBytes) { return send(host, port, msgBytes, null, null); }


    /**
     * 数据接收
     * @param bs 接收的字节
     * @param stream aio tcp 流
     */
    protected void receive(byte[] bs, AioStream stream) {}


    /**
     * 获取 一个可用 的 {@link AioStream}
     * @param host 主机
     * @param port 端口
     * @return {@link AioStream}
     */
    protected AioStream getSession(String host, Integer port) {
        String key = host + ":" + port;
        SafeList<AioStream> ls = streamMap.get(key);
        if (ls == null) {
            synchronized (streamMap) {
                ls = streamMap.get(key);
                if (ls == null) {
                    ls = new SafeList<>(); streamMap.put(key, ls);
                    ls.add(create(host, port));
                }
            }
        }

        AioStream stream = ls.findAny(se -> se.queue.size() < getInteger("maxWaitPerStream", 2));
        if (stream == null) {
            stream = create(host, port); ls.add(stream);
        }
        return stream;
    }


    /**
     * 创建 连接 AioStream
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
            channel.connect(new InetSocketAddress(host, port)).get(getLong("connectTimeout", 3000L), TimeUnit.MILLISECONDS);
            log.info("New TCP(AIO) connection to '" + key + "'");
        } catch(Exception ex) {
            try {channel.close();} catch(Exception exx) {}
            throw new RuntimeException("Connect error. " + key, ex);
        }
        final AioStream se = new AioStream(channel, this) {
            @Override
            protected void doClose(AioStream stream) { streamMap.get(key).remove(stream); }

            @Override
            protected void doRead(ByteBuffer buf) {
                if (delim == null) { // 没有分割符的时候
                    byte[] bs = new byte[buf.limit()];
                    buf.get(bs); buf.clear();
                    receive(bs, this);
                } else { // 分割 半包和粘包
                    do {
                        int delimIndex = indexOf(buf, delim);
                        if (delimIndex < 0) break;
                        int readableLength = delimIndex - buf.position();
                        byte[] bs = new byte[readableLength];
                        buf.get(bs);
                        receive(bs, this);

                        // 跳过 分割符的长度
                        for (int i = 0; i < delim.length; i++) { buf.get();}
                    } while (true);
                    buf.compact();
                }
            }
        };
        se.start();
        return se;
    }


    /**
     * 安全列表
     * @param <E>
     */
    protected class SafeList<E> {
        protected final LinkedList<E> data = new LinkedList<>();
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

        public boolean contains(E o) {
            try {
                lock.readLock().lock();
                return data.contains(o);
            } finally {
                lock.readLock().unlock();
            }
        }

        public boolean remove(E o) {
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
