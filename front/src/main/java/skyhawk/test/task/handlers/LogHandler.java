package skyhawk.test.task.handlers;

import static skyhawk.test.task.common.utils.Http.respond400;
import static skyhawk.test.task.common.utils.LogRecordUtil.getAggregationKey;
import static skyhawk.test.task.common.utils.LogRecordUtil.validate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.common.domain.Log;
import skyhawk.test.task.common.domain.TimeKey;
import skyhawk.test.task.common.utils.Env;
import skyhawk.test.task.service.RuntimeStore;
import skyhawk.test.task.service.kafka.KafkaWriter;

public class LogHandler implements HttpHandler {

  private static final Logger log = LoggerFactory.getLogger(LogHandler.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final KafkaWriter kafkaWriter = new KafkaWriter();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;
  private final String topicMain = Env.getKafkaTopicMain();

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {

    Log logRecord;
    try {
      logRecord = mapper.readValue(httpExchange.getRequestBody(), Log.class);
    } catch (IOException e) {
      respond400(httpExchange, Map.of("parsingErrors", List.of("request body is incorrect")));
      return;
    }

    final List<String> errors = validate(logRecord);
    if (!errors.isEmpty()) {
      respond400(httpExchange, Map.of("validationErrors", errors));
      return;
    }

    logRecord = new Log(
        logRecord.season().toUpperCase(),
        logRecord.team().toUpperCase(),
        logRecord.player().toUpperCase(),
        logRecord.points(),
        logRecord.rebounds(),
        logRecord.assists(),
        logRecord.steals(),
        logRecord.blocks(),
        logRecord.fouls(),
        logRecord.turnovers(),
        logRecord.minutesPlayed()
    );

    CacheRecord cacheRecord = new CacheRecord(logRecord, TimeKey.ofCurrentTime());

    try {
      final String key = getAggregationKey(cacheRecord.log());
      final byte[] value;
      try {
        value = mapper.writeValueAsBytes(cacheRecord);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      kafkaWriter.write(topicMain, key, value, Map.of(CacheRecord.HEADER_SENDER, Env.instanceId.getBytes())).get();
    } catch (Throwable e) {
      log.error("failed to write log", e);
      httpExchange.sendResponseHeaders(503, -1);
      return;
    }

    runtimeStore.log(cacheRecord);
    httpExchange.sendResponseHeaders(202, -1);
  }
}
