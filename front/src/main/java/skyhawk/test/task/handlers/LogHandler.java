package skyhawk.test.task.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class LogHandler implements HttpHandler {

  private final ObjectMapper mapper = new ObjectMapper();
  private final KafkaWriter kafkaWriter = new KafkaWriter();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;
  private final String topicMain = Env.getKafkaTopicMain();

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {

    final Log logRecord;
    try {
      logRecord = mapper.readValue(httpExchange.getRequestBody(), Log.class);
    } catch (IOException e) {
      respond400(httpExchange, Map.of("parsingErrors", List.of("request body is incorrect")));
      return;
    }

    final List<String> errors = logRecord.validate();
    if (!errors.isEmpty()) {
      respond400(httpExchange, Map.of("validationErrors", errors));
      return;
    }

    logRecord.setSeason(logRecord.getSeason().toUpperCase());
    logRecord.setTeam(logRecord.getTeam().toUpperCase());
    logRecord.setPlayer(logRecord.getPlayer().toUpperCase());

    CacheRecord cacheRecord = new CacheRecord(logRecord, TimeKey.ofCurrentTime());

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
      log.error("failed to write log", e);
      httpExchange.sendResponseHeaders(503, -1);
      return;
    }

    runtimeStore.log(cacheRecord);
    httpExchange.sendResponseHeaders(202, -1);
  }
}
