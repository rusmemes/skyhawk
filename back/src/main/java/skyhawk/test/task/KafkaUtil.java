package skyhawk.test.task;

import static skyhawk.test.task.DbUtil.insert;
import static skyhawk.test.task.common.utils.LogRecordUtil.getAggregationKey;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.kafka.KafkaReader;
import skyhawk.test.task.common.kafka.KafkaWriter;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.utils.Env;

public class KafkaUtil {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtil.class);

  private static final Comparator<CacheRecord> COMPARATOR = Comparator.comparing(CacheRecord::timeKey);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String removalTopic = Env.getKafkaTopicRemoval();
  private static final KafkaReader kafkaReader = new KafkaReader(Set.of(Env.getKafkaTopicMain()), false);
  private static final KafkaWriter kafkaWriter = new KafkaWriter();

  /**
   * Runs an infinite loop in the calling thread
   * */
  static void workOnKafka() throws InterruptedException {

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
        keyToValue.computeIfAbsent(record.key(), _ -> new ArrayList<>()).add(record.value());
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
            kafkaWriter
                .write(removalTopic, getAggregationKey(last.log()), bytes, Map.of())
                .get(1000, TimeUnit.SECONDS);
          } catch (SQLException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
          } catch (TimeoutException e) {
            log.warn("timeout while writing to removal topic", e);
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
}
