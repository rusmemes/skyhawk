package skyhawk.test.task.service.db;

import java.sql.Connection;
import java.sql.SQLException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import skyhawk.test.task.common.utils.Env;

public class DataSource {

  private static final HikariConfig config = new HikariConfig();
  private static final HikariDataSource ds;

  static {
    config.setJdbcUrl(Env.getDbUrl());
    config.setUsername(Env.getDbUsername());
    config.setPassword(Env.getDbPassword());
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    ds = new HikariDataSource(config);
  }

  public static Connection getConnection() throws SQLException {
    return ds.getConnection();
  }
}
