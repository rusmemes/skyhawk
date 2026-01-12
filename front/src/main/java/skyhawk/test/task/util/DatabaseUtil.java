package skyhawk.test.task.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.common.domain.Log;
import skyhawk.test.task.common.domain.TimeKey;
import skyhawk.test.task.domain.StatValue;
import skyhawk.test.task.service.db.DataSource;

public class DatabaseUtil {

  public static List<CacheRecord> loadFromDatabase(String season, Set<StatValue> columns) throws SQLException {

    String valueColumns = columns.stream().map(Enum::name).collect(Collectors.joining(","));

    final String sql = "select t1,t2,season,team,player,%s from nba_stats where season = ?".formatted(valueColumns);

    final List<CacheRecord> res = new ArrayList<>();
    try (Connection connection = DataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {

        statement.setString(1, season.strip().toUpperCase());

        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            int i;
            double d;
            res.add(
                new CacheRecord(
                    new Log(
                        resultSet.getString("season"),
                        resultSet.getString("team"),
                        resultSet.getString("player"),
                        columns.contains(StatValue.points)
                            && (i = resultSet.getInt(StatValue.points.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.rebounds)
                            && (i = resultSet.getInt(StatValue.rebounds.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.assists)
                            && (i = resultSet.getInt(StatValue.assists.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.steals)
                            && (i = resultSet.getInt(StatValue.steals.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.blocks)
                            && (i = resultSet.getInt(StatValue.blocks.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.fouls)
                            && (i = resultSet.getInt(StatValue.fouls.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.turnovers)
                            && (i = resultSet.getInt(StatValue.turnovers.name())) > -1
                            && !resultSet.wasNull()
                            ? i
                            : null,
                        columns.contains(StatValue.minutesPlayed)
                            && (d = resultSet.getDouble(StatValue.minutesPlayed.name())) > -1
                            && !resultSet.wasNull()
                            ? d
                            : null
                    ),
                    new TimeKey(
                        resultSet.getLong("t1"),
                        resultSet.getLong("t2")
                    )
                )
            );
          }
        }
      }
    }

    return res;
  }
}
