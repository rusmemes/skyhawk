package skyhawk.test.task.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import skyhawk.test.task.common.kafka.KafkaWriter;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.Log;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.common.utils.Env;
import skyhawk.test.task.runtime.store.RuntimeStore;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static skyhawk.test.task.common.utils.Http.respond400;

public class LogHandler implements HttpHandler {

  private final ObjectMapper mapper = new ObjectMapper();
  private final KafkaWriter kafkaWriter = new KafkaWriter();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;
  private final String topicMain = Env.getKafkaTopicMain();

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {

    final Log log;
    try {
      log = mapper.readValue(httpExchange.getRequestBody(), Log.class);
    } catch (IOException e) {
      respond400(httpExchange, Map.of("parsingErrors", List.of("request body is incorrect")));
      return;
    }

    final List<String> errors = log.validate();
    if (!errors.isEmpty()) {
      respond400(httpExchange, Map.of("validationErrors", errors));
      return;
    }

    log.setSeason(log.getSeason().toUpperCase());
    log.setTeam(log.getTeam().toUpperCase());
    log.setPlayer(log.getPlayer().toUpperCase());

    CacheRecord cacheRecord = new CacheRecord(log, TimeKey.ofCurrentTime());

    try {
      final String key = cacheRecord.log().getAggregationKey();
      final byte[] value;
      try {
        value = mapper.writeValueAsBytes(cacheRecord);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      kafkaWriter.write(topicMain, key, value, Map.of("sender", Env.instanceId.getBytes()));
    } catch (Throwable e) {
      e.printStackTrace();
      httpExchange.sendResponseHeaders(503, -1);
      return;
    }

    runtimeStore.log(cacheRecord);
    httpExchange.sendResponseHeaders(202, -1);
  }
}
