package cn.xnatural.aio;

import cn.xnatural.enet.event.EL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;


/**
 * TCP(AIO) 服务
 */
public class AioServer extends AioBase {
    protected static final Logger                                                  log         = LoggerFactory.getLogger(AioServer.class);
    /**
     * 新TCP连接 接受处理
     */
    protected final        CompletionHandler<AsynchronousSocketChannel, AioServer> acceptor    = new AcceptHandler();
    protected              AsynchronousServerSocketChannel                         ssc;
    /**
     * 绑定配置 hp -> host:port
     */
    protected
    final                  Integer                                                 port;
    protected final String                                                         hpCfg;
    // 当前连会话
    protected  final       Queue<AioStream>                                        connections = new ConcurrentLinkedQueue<>();
    protected final  Counter                                                       counter     = new Counter();
    /**
     * 数据分割符(半包和粘包)
     */
    protected final byte[]                              delim;


    public AioServer(Map<String, Object> attrs, ExecutorService exec) {
        super(attrs, exec);
        hpCfg = getStr("hp", ":7001");
        try {
            String delimiter = getStr("delimiter", null);
            if (delimiter != null && !delimiter.isEmpty()) delim = delimiter.getBytes("utf-8");
            else delim = null;
            port = Integer.valueOf(hpCfg.split(":")[1]);
        } catch (Exception ex) {
            throw new IllegalArgumentException("AioServer hp 格式错误. " + hpCfg, ex);
        }
    }


    /**
     * 启动
     */
    @EL(name = "sys.starting", async = true)
    public void start() {
        if (ssc != null) throw new RuntimeException("AioServer is already running");
        try {
            AsynchronousChannelGroup cg = AsynchronousChannelGroup.withThreadPool(exec);
            ssc = AsynchronousServerSocketChannel.open(cg);
            ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            ssc.setOption(StandardSocketOptions.SO_RCVBUF, getInteger("so_revbuf", 1024 * 1024));

            String host = hpCfg.split(":")[0];
            InetSocketAddress addr;
            if (host != null && !host.isEmpty()) {addr = new InetSocketAddress(host, port);}
            else addr = new InetSocketAddress(port);

            ssc.bind(addr, getInteger("backlog", 128));
            log.info("Start listen TCP(AIO) {}", port);
            accept();
        } catch (IOException ex) {
            throw new RuntimeException("Start error", ex);
        }
    }


    /**
     * 关闭
     */
    @EL(name = "sys.stopping", async = true)
    public void stop() {
        if (ssc != null) {
            try {
                ssc.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }


    /**
     * tcp byte 字节流接收处理
     * @param bs 接收到的字节
     * @param stream aio 流
     */
    protected void receive(byte[] bs, AioStream stream) {}


    /**
     * 接收新连接
     */
    protected void accept() {
        ssc.accept(this, acceptor);
    }


    @EL(name = {"aio.hp", "tcp.hp"}, async = false)
    public String getHp() {
        String ip = hpCfg.split(":")[0];
        if (ip == null || ip.isEmpty() || ip == "localhost") {ip = ipv4();}
        return ip + ":" + port;
    }


    /**
     * 获取本机 ip 地址
     * @return
     */
    protected String ipv4() {
        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
                NetworkInterface current = en.nextElement();
                if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
                Enumeration<InetAddress> addresses = current.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr.isLoopbackAddress()) continue;
                    if (addr instanceof Inet4Address) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            log.error("", e);
        }
        return null;
    }


    /**
     * 清除已关闭或已过期的连接
     * 接收系统心跳事件
     */
    @EL(name = "sys.heartbeat", async = true)
    protected void clean() {
        if (connections.isEmpty()) return;
        int size = connections.size();
        long expire = Duration.ofSeconds(getInteger("aioSession.maxIdle",
                ((Supplier<Integer>) () -> {
                    if (size > 80) return 60;
                    if (size > 50) return 120;
                    if (size > 30) return 180;
                    if (size > 20) return 300;
                    if (size > 10) return 400;
                    return 600;
                }).get()
        )).toMillis();

        int limit = ((Supplier<Integer>) () -> {
            if (size > 80) return 8;
            if (size > 50) return 5;
            if (size > 30) return 3;
            return 2;
        }).get();
        for (Iterator<AioStream> itt = connections.iterator(); itt.hasNext() && limit > 0; ) {
            AioStream se = itt.next();
            if (se == null) break;
            if (!se.channel.isOpen()) {
                itt.remove(); se.close();
                log.info("Cleaned unavailable AioSession: " + se + ", connected: " + connections.size());
            } else if (System.currentTimeMillis() - se.lastUsed > expire) {
                limit--; itt.remove(); se.close();
                log.info("Closed expired AioSession: " + se + ", connected: " + connections.size());
            }
        }
    }


    /**
     * 连接处理器
     */
    protected class AcceptHandler implements CompletionHandler<AsynchronousSocketChannel, AioServer> {

        @Override
        public void completed(final AsynchronousSocketChannel channel, final AioServer srv) {
            exec(() -> {
                AioStream se = null;
                try {
                    channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    channel.setOption(StandardSocketOptions.SO_RCVBUF, getInteger("so_rcvbuf", 1024 * 1024 * 2));
                    channel.setOption(StandardSocketOptions.SO_SNDBUF, getInteger("so_sndbuf", 1024 * 1024 * 2));
                    channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);

                    se = new AioStream(channel, srv) { //创建AioStream
                        @Override
                        protected void doClose(AioStream stream) {
                            connections.remove(stream);
                        }

                        @Override
                        protected void doRead(ByteBuffer bb) {
                            counter.increment(); // 统计
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
                    connections.offer(se);
                    InetSocketAddress rAddr = ((InetSocketAddress) channel.getRemoteAddress());
                    srv.log.info("New TCP(AIO) Connection from: " + rAddr.getHostString() + ":" + rAddr.getPort() + ", connected: " + connections.size());
                    se.start();
                    if (connections.size() > 10) clean();
                } catch (IOException e) {
                    if (se != null) se.close();
                    else {
                        try { channel.close(); } catch (IOException ex) {}
                    }
                    log.error("Create AioStream error", e);
                }
            });
            // 继续接入新连接
            srv.accept();
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

        @Override
        public void failed(Throwable ex, AioServer srv) {
            if (!(ex instanceof ClosedChannelException)) {
                srv.log.error(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage(), ex);
            }
        }
    }


    /**
     * 统计每小时的处理 tcp 数据包个数
     * MM-dd HH -> 个数
     */
    protected class Counter {
        protected final Map<String, LongAdder> hourCount = new ConcurrentHashMap<>(3);
        public void increment() {
            SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH");
            boolean isNew = false;
            String hStr = sdf.format(new Date());
            LongAdder count = hourCount.get(hStr);
            if (count == null) {
                synchronized (hourCount) {
                    count = hourCount.get(hStr);
                    if (count == null) {
                        count = new LongAdder(); hourCount.put(hStr, count);
                        isNew = true;
                    }
                }
            }
            count.increment();
            if (isNew) {
                final Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.HOUR_OF_DAY, -1);
                String lastHour = sdf.format(cal.getTime());
                LongAdder c = hourCount.remove(lastHour);
                if (c != null) log.info("{} 时共处理 TCP(AIO) 数据包: {} 个", lastHour, c);
            }
        }
    }
}
