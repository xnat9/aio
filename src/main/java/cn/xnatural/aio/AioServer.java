package cn.xnatural.aio;

import cn.xnatural.enet.event.EL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class AioServer extends AioBase {
    protected static final Logger                                                  log         = LoggerFactory.getLogger(AioServer.class);
    /**
     * 接收TCP消息的处理函数集
     */
    protected final        List<BiConsumer<String, AioStream>>                     msgFns      = new LinkedList<>();
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


    public AioServer(Map<String, Object> attrs, ExecutorService exec) {
        super(attrs, exec);
        hpCfg = getStr("hp", ":7001");
        try {
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
            InetSocketAddress addr = new InetSocketAddress(port);
            if (host != null && !host.isEmpty()) {addr = new InetSocketAddress(host, port);}

            ssc.bind(addr, getInteger("backlog", 128));
            log.info("Start listen TCP(AIO) {}", port);
            msgFns.add((msg, se) -> receive(msg, se));
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
     * 添加消息处理函数
     * @param msgFn 入参1: 是消息字符串, 入参2: 回响函数
     * @return
     */
    public AioServer msgFn(BiConsumer<String, AioStream> msgFn) {if (msgFn != null) this.msgFns.add(msgFn); return this; }


    /**
     * 消息接受处理
     * @param msg 消息内容
     * @param stream AioSession
     */
    protected void receive(String msg, AioStream stream) {
        try {
            log.trace("Receive client '{}' data: {}", stream.channel.getRemoteAddress(), msg);
        } catch (IOException e) {
            log.error("", e);
        }
        counter.increment(); // 统计
    }


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
