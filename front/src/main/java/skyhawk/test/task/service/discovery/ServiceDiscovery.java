package skyhawk.test.task.service.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.db.DataSource;
import skyhawk.test.task.common.utils.Env;

public class ServiceDiscovery {

  private static final Logger log = LoggerFactory.getLogger(ServiceDiscovery.class);

  private static final String DDL = """
      create table if not exists service_discovery
      (
          url                 text      not null,
          last_heartbeat_time timestamp not null
      );
      create unique index if not exists service_discovery_url_unique_idx ON service_discovery (url);
      """;

  private static final ServiceDiscovery instance;

  public static ServiceDiscovery getInstance() {
    return instance;
  }

  static {
    try {
      instance = new ServiceDiscovery(new URI(Env.getServiceDiscoverySelfUrl()), Env.getServiceDiscoveryExpirationTime());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    try (Connection connection = DataSource.getConnection()) {
      runDDL(connection);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    long interval = Env.getServiceDiscoveryHeartbeatIntervalMs();

    Thread.ofVirtual().start(() -> {
      while (true) {
        try (Connection connection = DataSource.getConnection()) {
          instance.syncState(connection);
        } catch (SQLException | URISyntaxException e) {
          log.error("an error occurred while syncing service discovery", e);
        }
        try {
          Thread.sleep(interval);
        } catch (InterruptedException ignored) {
          break;
        }
      }
    });
  }

  private final URI self;
  private final long expirationTimeMs;

  private final CopyOnWriteArrayList<URI> serviceList = new CopyOnWriteArrayList<>();

  public ServiceDiscovery(URI self, long expirationTimeMs) {
    this.self = self;
    this.expirationTimeMs = expirationTimeMs;
  }

  public static void runDDL(Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      statement.executeUpdate(DDL);
    }
  }

  private void syncState(Connection connection) throws SQLException, URISyntaxException {
    heartbeat(connection);
    workOnState(getState(connection));
  }

  public void reportBadURI(URI uri) {
    serviceList.remove(uri);
  }

  public List<URI> getServiceList() {
    return Collections.unmodifiableList(serviceList);
  }

  private void workOnState(List<URI> actualState) {
    for (int i = 0; i < actualState.size(); i++) {
      final URI uri = actualState.get(i);
      if (serviceList.size() > i) {
        serviceList.set(i, uri);
      } else {
        serviceList.add(uri);
      }
    }

    while (serviceList.size() > actualState.size()) {
      serviceList.removeLast();
    }
  }

  private void heartbeat(Connection connection) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(
        """
            insert into service_discovery (url, last_heartbeat_time) values (?, ?)
            on conflict (url) do update set last_heartbeat_time = excluded.last_heartbeat_time
            """
    )) {
      preparedStatement.setString(1, self.toString());
      preparedStatement.setTimestamp(2, Timestamp.from(Instant.now()));
      preparedStatement.execute();
    }
  }

  private List<URI> getState(Connection connection) throws SQLException, URISyntaxException {
    List<URI> state = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement(
        "select url from service_discovery where url != ? and last_heartbeat_time > ?"
    )) {
      preparedStatement.setString(1, self.toString());
      preparedStatement.setTimestamp(2, Timestamp.from(Instant.now().minus(expirationTimeMs, ChronoUnit.MILLIS)));
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          state.add(new URI(resultSet.getString("url")));
        }
      }
    }
    return state;
  }
}
