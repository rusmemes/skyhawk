package skyhawk.test.task;

import static skyhawk.test.task.DbUtil.runDDL;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.util.concurrent.Executors;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.db.DataSource;
import skyhawk.test.task.common.utils.Env;

public class Back {

  private static final Logger log = LoggerFactory.getLogger(Back.class);

  static void main() throws Throwable {

    try (Connection connection = DataSource.getConnection()) {
      runDDL(connection);
    }
    log.info("DDL applied");

    final HttpServer server = HttpServer.create(new InetSocketAddress(Env.getPort()), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext("/health", ex -> ex.sendResponseHeaders(200, -1));
    server.start();

    log.info("Health endpoint started");

    try {
      KafkaUtil.workOnKafka();
    } finally {
      server.stop(0);
    }
  }
}
