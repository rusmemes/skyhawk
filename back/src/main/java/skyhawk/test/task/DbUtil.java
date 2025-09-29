package skyhawk.test.task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import skyhawk.test.task.common.db.DataSource;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.Log;
import skyhawk.test.task.common.protocol.TimeKey;

public class DbUtil {

  private static final String DDL = """
    create table if not exists nba_stats
    (
        t1            bigint not null,
        t2            bigint not null,
        season        text   not null,
        team          text   not null,
        player        text   not null,
        points        integer,
        rebounds      integer,
        assists       integer,
        steals        integer,
        blocks        integer,
        fouls         integer,
        turnovers     integer,
        minutesPlayed decimal(10, 4)
    );
    create unique index if not exists nba_stats_unique_idx ON nba_stats (season,player,team,t1,t2);
    create index if not exists nba_stats_agg_idx ON nba_stats (season,player,team);
    """;

  static void insert(List<CacheRecord> records) throws SQLException {
    if (records.isEmpty()) {
      return;
    }

    try (Connection connection = DataSource.getConnection()) {
      connection.setAutoCommit(false);

      {
        StringBuilder builder = initBuilder();
        List<CacheRecord> chunk = new ArrayList<>();

        for (CacheRecord record : records) {
          addRecord(builder);
          chunk.add(record);
          if (builder.length() > 1000) {

            insert(builder, connection, chunk);

            builder = initBuilder();
            chunk = new ArrayList<>();
          }
        }

        if (!chunk.isEmpty()) {
          insert(builder, connection, chunk);
        }
      }

      connection.commit();
    }
  }

  private static void insert(
      StringBuilder builder,
      Connection connection,
      List<CacheRecord> chunk
  ) throws SQLException {

    String sql = finishInsertSql(builder);

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

      int index = 1;
      for (CacheRecord cacheRecord : chunk) {
        final TimeKey timeKey = cacheRecord.timeKey();
        preparedStatement.setLong(index++, timeKey.t1());
        preparedStatement.setLong(index++, timeKey.t2());

        final Log log = cacheRecord.log();

        preparedStatement.setString(index++, log.season());
        preparedStatement.setString(index++, log.team());
        preparedStatement.setString(index++, log.player());

        setNullableInteger(log.points(), index++, preparedStatement);
        setNullableInteger(log.rebounds(), index++, preparedStatement);
        setNullableInteger(log.assists(), index++, preparedStatement);
        setNullableInteger(log.steals(), index++, preparedStatement);
        setNullableInteger(log.blocks(), index++, preparedStatement);
        setNullableInteger(log.fouls(), index++, preparedStatement);
        setNullableInteger(log.turnovers(), index++, preparedStatement);

        final Double minutesPlayed = log.minutesPlayed();
        if (minutesPlayed != null) {
          preparedStatement.setDouble(index++, minutesPlayed);
        } else {
          preparedStatement.setNull(index++, Types.DOUBLE);
        }
      }

      preparedStatement.execute();
    }
  }

  private static String finishInsertSql(StringBuilder builder) {
    builder.setLength(builder.length() - 1);
    builder.append(" on conflict do nothing;");
    return builder.toString();
  }

  static void runDDL(Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      statement.executeUpdate(DDL);
    }
  }

  private static StringBuilder initBuilder() {
    return new StringBuilder(
        "insert into nba_stats (t1, t2, season, team, player, points, rebounds, assists, steals, blocks, fouls, turnovers, minutesPlayed) values "
    );
  }

  private static void addRecord(StringBuilder builder) {
    builder.append("(").append("?,?,?,?,?,?,?,?,?,?,?,?,?").append("),");
  }

  private static void setNullableInteger(
      Integer integer,
      int index,
      PreparedStatement preparedStatement
  ) throws SQLException {
    if (integer == null) {
      preparedStatement.setNull(index, java.sql.Types.INTEGER);
    } else {
      preparedStatement.setInt(index, integer);
    }
  }
}
