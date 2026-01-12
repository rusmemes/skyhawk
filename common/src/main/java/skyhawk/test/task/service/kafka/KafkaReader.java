package skyhawk.test.task.service.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import skyhawk.test.task.common.utils.Env;

public class KafkaReader {

  private final boolean autoCommit;
  private final Consumer<String, byte[]> consumer;

  public KafkaReader(Set<String> topics, boolean autoCommit) {

    this.autoCommit = autoCommit;
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.getKafkaBootstrapServers());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);

    String groupId = Env.getKafkaGroupId();
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        groupId.equalsIgnoreCase("random") ? UUID.randomUUID().toString() : groupId
    );

    this.consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(topics);
  }

  public ConsumerRecords<String, byte[]> read() {
    try {
      return consumer.poll(Duration.ofMillis(10));
    } catch (Throwable e) {
      if (e instanceof WakeupException) {
        return ConsumerRecords.empty();
      }
      throw new RuntimeException(e);
    }
  }

  public void commitOffset() {
    if (!autoCommit) {
      consumer.commitSync();
    }
  }
}
