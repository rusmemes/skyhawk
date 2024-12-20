package skyhawk.test.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import skyhawk.test.task.common.db.DataSource;
import skyhawk.test.task.common.kafka.KafkaReader;
import skyhawk.test.task.common.kafka.KafkaWriter;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.Log;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.common.utils.Env;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

@Slf4j
public class Back {

  private static final Comparator<CacheRecord> COMPARATOR = Comparator.comparing(CacheRecord::timeKey);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String removalTopic = Env.getKafkaTopicRemoval();
  private static final KafkaReader kafkaReader = new KafkaReader(Set.of(Env.getKafkaTopicMain()), false);
  private static final KafkaWriter kafkaWriter = new KafkaWriter();

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

  public static void main(String[] args) throws Throwable {

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
      workOnKafka();
    } finally {
      server.stop(0);
    }
  }

  private static void workOnKafka() throws InterruptedException {

    boolean firstRead = true;

    while (true) {

      final ConsumerRecords<String, byte[]> poll = kafkaReader.read();
      if (firstRead) {
        firstRead = false;
        log.info("First batch read");
      }

      if (poll.isEmpty()) {
        continue;
      }

      // group by keys
      Map<String, List<byte[]>> keyToValue = new HashMap<>();
      for (final ConsumerRecord<String, byte[]> record : poll) {
        keyToValue.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record.value());
      }

      // process each record group concurrently
      final List<Thread> threads = new ArrayList<>(keyToValue.size());
      for (List<byte[]> list : keyToValue.values()) {
        threads.add(Thread.ofVirtual().start(() -> {
          try {
            final List<CacheRecord> records = parseRecords(list);
            insert(records);

            final CacheRecord last = records.getLast();
            final byte[] bytes;
            try {
              bytes = MAPPER.writeValueAsBytes(last);
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
            kafkaWriter.write(removalTopic, last.log().getAggregationKey(), bytes, Map.of());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      for (Thread thread : threads) {
        // it's not a problem to stop the whole process if case of some errors as the processing is idempotent
        thread.join();
      }

      kafkaReader.commitOffset();
    }
  }

  private static List<CacheRecord> parseRecords(List<byte[]> list) {
    final List<CacheRecord> records = new ArrayList<>(list.size());
    for (byte[] bytes : list) {
      final CacheRecord cacheRecord;
      try {
        cacheRecord = MAPPER.readValue(bytes, CacheRecord.class);
      } catch (IOException e) {
        log.error("failed to parse cache record", e);
        continue;
      }
      records.add(cacheRecord);
    }

    records.sort(COMPARATOR);

    return records;
  }

  private static void insert(List<CacheRecord> records) throws SQLException {
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

  private static void insert(StringBuilder builder, Connection connection, List<CacheRecord> chunk) throws SQLException {
    String sql = finishInsertSql(builder);

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

      int index = 1;
      for (CacheRecord cacheRecord : chunk) {
        final TimeKey timeKey = cacheRecord.timeKey();
        preparedStatement.setLong(index++, timeKey.t1());
        preparedStatement.setLong(index++, timeKey.t2());

        final Log log = cacheRecord.log();

        preparedStatement.setString(index++, log.getSeason());
        preparedStatement.setString(index++, log.getTeam());
        preparedStatement.setString(index++, log.getPlayer());

        setNullableInteger(log.getPoints(), index++, preparedStatement);
        setNullableInteger(log.getRebounds(), index++, preparedStatement);
        setNullableInteger(log.getAssists(), index++, preparedStatement);
        setNullableInteger(log.getSteals(), index++, preparedStatement);
        setNullableInteger(log.getBlocks(), index++, preparedStatement);
        setNullableInteger(log.getFouls(), index++, preparedStatement);
        setNullableInteger(log.getTurnovers(), index++, preparedStatement);

        final Double minutesPlayed = log.getMinutesPlayed();
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

  private static void runDDL(Connection conn) throws SQLException {
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

  private static void setNullableInteger(Integer integer, int index, PreparedStatement preparedStatement) throws SQLException {
    if (integer != null) {
      preparedStatement.setInt(index, integer);
    } else {
      preparedStatement.setNull(index, java.sql.Types.INTEGER);
    }
  }
}
