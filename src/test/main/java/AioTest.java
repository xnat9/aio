import cn.xnatural.aio.AioClient;
import cn.xnatural.aio.AioServer;
import cn.xnatural.aio.AioStream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class AioTest {

    public static void main(String[] args) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "aio-" + i.getAndIncrement());
            }
        });
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("delimiter", "\n");
        AioServer server = new AioServer(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) {
                log.info("服务端 -> 接收数据: " + new String(bs) + ", length: " + bs.length);
                try {
                    Thread.sleep(1000 * 4);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stream.reply(
                        ("服务端 回消息: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).getBytes(),
                        (ex, me) -> { //失败回调函数
                            log.error("服务端 -> 回消息 失败", ex);
                        }, () -> { //成功回调函数
                            log.info("服务端 -> 回消息 成功");
                        });
            }
        };
        server.start();

        AioClient client = new AioClient(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) { //数据接收
                log.info("客户端 -> 接收数据: " + new String(bs) + ", length: " + bs.length);
                try {
                    Thread.sleep(1000 * 2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stream.reply(
                        ("客户端 -> 回消息: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).getBytes()
                );
            }
        };
        client.send("localhost", 7001, "Hello1".getBytes());
        // client.send("localhost", 7001, "Hello2".getBytes());

        Thread.sleep(1000 * 30 * 1);
        server.stop();
        client.stop();
        // exec.shutdown();
    }
}
