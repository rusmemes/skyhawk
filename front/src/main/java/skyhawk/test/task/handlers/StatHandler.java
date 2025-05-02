package skyhawk.test.task.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.stat.StatRequest;
import skyhawk.test.task.stat.StatValue;
import skyhawk.test.task.util.DatabaseUtil;
import skyhawk.test.task.util.ServiceCallUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static skyhawk.test.task.common.utils.Http.respond400;
import static skyhawk.test.task.util.StatUtils.calcStats;
import static skyhawk.test.task.util.StatUtils.mergeResultToCollector;

public class StatHandler implements HttpHandler {

  private static final Logger log = LoggerFactory.getLogger(StatHandler.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final ServiceCallUtil serviceCallUtil = new ServiceCallUtil();

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // parse request
    final StatRequest statRequest;
    try {
      statRequest = mapper.readValue(exchange.getRequestBody(), StatRequest.class);
    } catch (Throwable e) {
      respond400(exchange, Map.of("parsingErrors", List.of("request body is incorrect")));
      return;
    }

    // validate request
    if (statRequest.season() == null
        || statRequest.season().isBlank()
        || statRequest.per() == null
        || statRequest.values() == null
        || statRequest.values().isEmpty()
        || statRequest.values().contains(null)
    ) {
      respond400(exchange, Map.of(
          "validationErrors",
          "request body is incorrect: provide keys and values description"
      ));
      return;
    }

    final Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> processedRequest = processRequest(
        statRequest.season(),
        statRequest.values()
    );

    if (processedRequest == null) {
      exchange.sendResponseHeaders(204, -1);
      return;
    }

    final Map<String, Map<StatValue, Number>> response = calcStats(
        processedRequest.getOrDefault(statRequest.season().toUpperCase(), Map.of()),
        statRequest.values(),
        statRequest.per()
    );

    if (response.isEmpty()) {
      exchange.sendResponseHeaders(204, -1);
    } else {
      final byte[] responseBody = mapper.writeValueAsBytes(response);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, responseBody.length);
      try (OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(responseBody);
      }
    }
  }

  public Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> processRequest(
      String season,
      Set<StatValue> columns
  ) {
    // map to collect raw data both from the database and other backs:
    // season -> team -> player -> time -> record
    Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> rawDataCollector = new HashMap<>();

    // call other backs
    CompletableFuture<List<CacheRecord>> res = serviceCallUtil.callServices(season);

    // load from database
    try {
      // merge the data from the db
      mergeResultToCollector(DatabaseUtil.loadFromDatabase(season, columns), rawDataCollector);
    } catch (SQLException e) {
      log.error("failed to load and merge results", e);
      return null;
    }

    // merge the data from other backs
    mergeResultToCollector(res.join(), rawDataCollector);
    return rawDataCollector;
  }
}
