package cn.xnatural.aio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * 一条AIO tcp连接会话, 数据流
 */
public class AioStream {
    protected static final Logger                       log         = LoggerFactory.getLogger(AioStream.class);
    /**
     * Aio 监听渠道
     */
    protected final AsynchronousSocketChannel           channel;
    /**
     * Aio tcp socket 流 读取器
     */
    protected final ReadHandler     readHandler = new ReadHandler();
    /**
     * {@link AioClient} {@link AioServer}
     */
    public final AioBase delegate;
    /**
     * Write 任务对列
     */
    protected final Queue<Runnable> queue       = new ConcurrentLinkedQueue<>();
    /**
     * 上次读写时间
     */
    protected       Long            lastUsed    = System.currentTimeMillis();
    /**
     * 是否已关闭
     */
    protected final AtomicBoolean   closed      = new AtomicBoolean(false);
    /**
     * 是否正在写入
     */
    protected final AtomicBoolean   writing     = new AtomicBoolean(false);
    /**
     * 每次接收消息的内存空间
     */
    protected final ByteBuffer      buf;
    /**
     * 数据分割符(半包和粘包)
     */
    protected final byte[]                              delim;


    /**
     * 创建 {@link AioStream}
     * @param channel {@link AsynchronousSocketChannel}
     * @param delegate {@link AioClient} or {@link AioServer}
     */
    public AioStream(AsynchronousSocketChannel channel, AioBase delegate) {
        if (channel == null) throw new NullPointerException("channel must not be null");
        if (delegate == null) throw new NullPointerException("exec must not be null");
        this.channel = channel;
        this.delegate = delegate;
        this.buf = ByteBuffer.allocate(delegate.getInteger("maxMsgSize", 1024 * 1024));
        this.delim = (byte[]) delegate.getAttr("delim");
    }


    /**
     * 开始数据接收处理
     */
    public void start() { read(); }


    /**
     * 关闭
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (int i = 0; !queue.isEmpty() && i < 3; i++) { //如果有数据没处理完 则稍等
                try { Thread.sleep(500); } catch (InterruptedException e) {/** ignore **/}
            }
            try { channel.shutdownOutput(); } catch(Exception ex) {}
            try { channel.shutdownInput(); } catch(Exception ex) {}
            try { channel.close(); } catch(Exception ex) {}
            doClose(this);
        }
    }


    /**
     * 子类重写, 清除对当前{@link AioStream}的引用
     * @param stream
     */
    protected void doClose(AioStream stream) {}


    /**
     * 子类 重写 读数据
     * @param buf
     */
    protected void doRead(ByteBuffer buf) {}


    /**
     * 写入消息到流
     * @param data 要写入的数据
     * @param failFn 失败回调函数
     * @param okFn 成功回调函数
     */
    protected void write(ByteBuffer data, BiConsumer<Exception, AioStream> failFn, Runnable okFn) {
        if (data == null) throw new IllegalArgumentException("Write data us empty");
        lastUsed = System.currentTimeMillis();
        queue.offer(() -> { // 排对发送消息. 避免 WritePendingException
            Exception exx = null;
            try {
                channel.write(data).get(delegate.getInteger("writeTimeout", 10000), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                exx = ex;
                close();
                if (failFn != null) delegate.exec(() -> failFn.accept(ex, this));
                else {
                    if (!(ex instanceof ClosedChannelException)) {
                        try {
                            log.error(channel.getLocalAddress().toString() + " ->" + channel.getRemoteAddress().toString(), ex);
                        } catch (IOException e) {
                            log.error("", e);
                        }
                    }
                }
            }
            if (okFn != null && exx == null) delegate.exec(okFn);
        });
        trigger();
    }


    /**
     * 回应消息
     * @param bs 消息字节数组
     * @param failFn 回应失败回调函数
     * @param okFn 回应成功回调函数
     */
    public void reply(byte[] bs, BiConsumer<Exception, AioStream> failFn, Runnable okFn) {
        if (delim == null) {
            write(ByteBuffer.wrap(bs), failFn, okFn);
        } else {
            ByteBuffer msgBuf = ByteBuffer.allocate(bs.length + delim.length);
            msgBuf.put(bs); msgBuf.put(delim); msgBuf.flip();
            write(msgBuf, failFn, okFn);
        }
    }


    /**
     * {@link #reply(byte[], BiConsumer, Runnable)}
     * @param bs 消息字节数组
     */
    public void reply(byte[] bs) {
        if (delim == null) {
            write(ByteBuffer.wrap(bs), null, null);
        } else {
            ByteBuffer msgBuf = ByteBuffer.allocate(bs.length + delim.length);
            msgBuf.put(bs); msgBuf.put(delim); msgBuf.flip();
            write(msgBuf, null, null);
        }
    }



    /**
     * 遍历消息对列发送
     */
    protected void trigger() { // 触发发送
        if (closed.get() || queue.isEmpty()) return;
        if (!writing.compareAndSet(false, true)) return;
        delegate.exec(() -> {
            try {
                Runnable task = queue.poll();
                if (task != null) task.run();
            } finally {
                writing.set(false);
                if (!queue.isEmpty()) trigger(); // 持续不断执行对列中的任务
            }
        });
    }


    /**
     * 继续处理接收数据
     */
    protected void read() {
        if (closed.get()) return;
        channel.read(buf, buf, readHandler);
    }


    /**
     * 远程连接地址
     * @return
     */
    public String getRemoteAddress() {
        try {
            return channel.getRemoteAddress().toString();
        } catch (IOException e) {
            log.error("",e);
        }
        return null;
    }


    /**
     * 本地连接地址
     * @return
     */
    public String getLocalAddress() {
        try {
            return channel.getLocalAddress().toString();
        } catch (IOException e) {
            log.error("",e);
        }
        return null;
    }


    @Override
    public String toString() { return super.toString() + "[" + channel.toString() + "]"; }



    /**
     * socket 数据读取处理器
     */
    protected class ReadHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer count, ByteBuffer buf) {
            lastUsed = System.currentTimeMillis();
            if (count > 0) {
                buf.flip();
                doRead(buf);
                try { // 同一时间只有一个 read, 避免 ReadPendingException
                    read();
                } catch (Exception ex) {
                    log.error(delegate.getClass().getName(), ex);
                    close();
                }
            } else {
                // 接收字节为空
                if (!channel.isOpen()) close();
            }
        }


        @Override
        public void failed(Throwable ex, ByteBuffer buf) {
            if (!(ex instanceof ClosedChannelException)) {
                log.error("", ex);
            }
            close();
        }
    }
}
