package skyhawk.test.task;

import static skyhawk.test.task.DbUtil.runDDL;

import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.db.DataSource;

public class Back {

  private static final Logger log = LoggerFactory.getLogger(Back.class);

  static void main() throws Throwable {

    try (Connection connection = DataSource.getConnection()) {
      runDDL(connection);
    }
    log.info("DDL applied");

    try {
      // blocking call
      KafkaUtil.workOnKafka();
    } catch (Exception e) {
      // in case of an error the entire application is getting stopped and the health endpoint is stoping to respond
      // so the application must be restarted externally
      log.error("Error while working on Kafka, server is getting stopped", e);
    }
  }
}
