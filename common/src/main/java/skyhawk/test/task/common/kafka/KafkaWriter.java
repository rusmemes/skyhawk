package skyhawk.test.task.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import skyhawk.test.task.common.utils.Env;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaWriter {

  private final KafkaProducer<String, byte[]> producer;

  public KafkaWriter() {

    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.getKafkaBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, Env.instanceId);

    this.producer = new KafkaProducer<>(properties);
  }

  public void write(String topic, String key, byte[] value, Map<String, byte[]> headers) {

    final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
    headers.forEach(record.headers()::add);

    try {
      producer.send(record).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
