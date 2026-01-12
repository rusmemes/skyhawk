package skyhawk.test.task.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.common.utils.Env;
import skyhawk.test.task.service.kafka.KafkaReader;

public class KafkaWorker {

  private static final Logger log = LoggerFactory.getLogger(KafkaWorker.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String mainTopic = Env.getKafkaTopicMain();
  private static final String removalTopic = Env.getKafkaTopicRemoval();
  private static final KafkaReader kafkaReader = new KafkaReader(Set.of(mainTopic, removalTopic), true);
  private static final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;

  public static void runKafkaWorker() {
    Thread.ofVirtual().start(() -> {
      while (true) {
        final ConsumerRecords<String, byte[]> records;
        try {
          records = kafkaReader.read();
        } catch (Throwable e) {
          log.error("Failed to read records from kafka", e);
          System.exit(1);
          break;
        }
        records.forEach(KafkaWorker::processIncomingKafkaRecord);
      }
    });
  }

  private static void processIncomingKafkaRecord(ConsumerRecord<String, byte[]> record) {
    final String topic = record.topic();
    if (topic.equals(mainTopic)) {
      processRecordFromMain(record);
    } else if (topic.equals(removalTopic)) {
      processRecordFromRemoval(record);
    }
  }

  private static void processRecordFromRemoval(ConsumerRecord<String, byte[]> record) {
    final CacheRecord cacheRecord = parseRecord(record);
    if (cacheRecord != null) {
      runtimeStore.remove(cacheRecord);
    }
  }

  private static void processRecordFromMain(ConsumerRecord<String, byte[]> record) {
    if (!isSentByMe(record)) {
      final CacheRecord cacheRecord = parseRecord(record);
      if (cacheRecord != null) {
        runtimeStore.log(cacheRecord);
      }
    }
  }

  private static CacheRecord parseRecord(ConsumerRecord<String, byte[]> record) {
    final byte[] bytes = record.value();
    CacheRecord cacheRecord;
    try {
      cacheRecord = MAPPER.readValue(bytes, CacheRecord.class);
    } catch (IOException e) {
      log.error("Failed to parse cacheRecord", e);
      return null;
    }
    return cacheRecord;
  }

  private static boolean isSentByMe(ConsumerRecord<String, byte[]> record) {
    final Iterable<Header> sender = record.headers().headers(CacheRecord.HEADER_SENDER);
    final Iterator<Header> iterator = sender == null ? null : sender.iterator();
    if (iterator != null && iterator.hasNext()) {
      final byte[] bytes = iterator.next().value();
      return Arrays.equals(Env.instanceId.getBytes(), bytes);
    }
    return false;
  }
}
