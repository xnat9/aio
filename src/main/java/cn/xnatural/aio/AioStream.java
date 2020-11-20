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
    protected final    AsynchronousSocketChannel        channel;
    protected final ReadHandler                         readHandler = new ReadHandler();
    protected       AioBase                             aioBase;
    protected final Queue<Runnable>                     queue       = new ConcurrentLinkedQueue<>();
    // close 回调函数
    protected       Runnable                            closeFn;
    protected       Long                                lastUsed    = System.currentTimeMillis();
    // 上次读写时间
    protected final AtomicBoolean                       closed      = new AtomicBoolean(false);
    protected final        AtomicBoolean                running     = new AtomicBoolean(false);
    // 每次接收消息的内存空间
    protected final    ByteBuffer                       buf;


    /**
     * 创建 AioSession
     * @param channel {@link AsynchronousSocketChannel}
     * @param aioBase
     */
    public AioStream(AsynchronousSocketChannel channel, AioBase aioBase) {
        if (channel == null) throw new IllegalArgumentException("channel must not be null");
        if (aioBase == null) throw new IllegalArgumentException("exec must not be null");
        this.channel = channel;
        this.aioBase = aioBase;
        this.buf = ByteBuffer.allocate(aioBase.getInteger("maxMsgSize", 1024 * 1024));
    }


    protected void doRead(ByteBuffer bb) {

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
            try { channel.shutdownOutput(); } catch(Exception ex) {}
            try { channel.shutdownInput(); } catch(Exception ex) {}
            try { channel.close(); } catch(Exception ex) {}
            doClose(this);
            if (closeFn != null) closeFn.run();
        }
    }


    protected void doClose(AioStream session) {
        // TODO
    }


    /**
     * 当前会话渠道是否忙
     * @return
     */
    public boolean busy() { return queue.size() > 1; }


    /**
     * 发送消息到客户端
     * @param bb
     * @param failFn 失败回调
     * @param okFn 成功回调
     */
    public void write(ByteBuffer bb, BiConsumer<Exception, AioStream> failFn, Runnable okFn) {
        if (closed.get() || bb == null) return;
        lastUsed = System.currentTimeMillis();
        queue.offer(() -> { // 排对发送消息. 避免 WritePendingException
            try {
                channel.write(bb).get(1000L, TimeUnit.MILLISECONDS);
                if (okFn != null) okFn.run();
            } catch (Exception ex) {
                close();
                if (failFn != null) failFn.accept(ex, this);
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
        });
        trigger();
    }


    /**
     * {@link #write(ByteBuffer, BiConsumer, Runnable)}
     * @param bb
     */
    public void write(ByteBuffer bb) {
        write(bb, null, null);
    }


    /**
     * 遍历消息对列发送
     */
    protected void trigger() { // 触发发送
        if (queue.isEmpty()) return;
        if (!running.compareAndSet(false, true)) return;
        aioBase.exec(() -> {
            Runnable task;
            try {
                task = queue.poll();
                if (task != null) task.run();
            } finally {
                running.set(false);
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


    @Override
    public String toString() {
        return super.toString() + "[" + channel.toString() + "]";
    }


    /**
     * socket 数据读取处理器
     */
    protected class ReadHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer count, ByteBuffer buf) {
            if (count > 0) {
                lastUsed = System.currentTimeMillis();
                buf.flip();
                doRead(buf);
                // 同一时间只有一个 read, 避免 ReadPendingException
                read();
            } else {
                // log.warn("接收字节为空. 关闭 " + channel.toString());
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
