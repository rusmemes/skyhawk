package skyhawk.test.task;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.utils.Env;
import skyhawk.test.task.handlers.LogHandler;
import skyhawk.test.task.handlers.StatCopyHandler;
import skyhawk.test.task.handlers.StatHandler;
import skyhawk.test.task.service.KafkaWorker;

public class Front {

  private static final Logger log = LoggerFactory.getLogger(Front.class);

  static void main() throws IOException {
    KafkaWorker.runKafkaWorker();
    startHttpServer();
  }

  private static void startHttpServer() throws IOException {
    int port = Env.getPort();
    HttpServer server = HttpServer.create(new InetSocketAddress(port), Env.getSocketBacklog());

    server.createContext("/log", new LogHandler());
    server.createContext("/stat", new StatHandler());
    server.createContext("/stat-copy", new StatCopyHandler());

    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

    server.start();

    log.info("Http server started on port {}", port);
  }
}
