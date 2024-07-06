package skyhawk.test.task.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.common.protocol.stat.StatKey;
import skyhawk.test.task.common.protocol.stat.StatRequest;
import skyhawk.test.task.common.protocol.stat.StatValue;
import skyhawk.test.task.common.protocol.stat.StatValueAggFunction;
import skyhawk.test.task.util.DatabaseUtil;
import skyhawk.test.task.util.ServiceCallUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static skyhawk.test.task.common.utils.Http.respond400;
import static skyhawk.test.task.util.StatUtils.calcStats;
import static skyhawk.test.task.util.StatUtils.convertToKeysMap;
import static skyhawk.test.task.util.StatUtils.convertToValuesMap;
import static skyhawk.test.task.util.StatUtils.mergeResultToCollector;

public class StatHandler implements HttpHandler {

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
    if (statRequest.getKeys() == null
        || statRequest.getKeys().isEmpty()
        || statRequest.getKeys().stream().anyMatch(statRequestKey -> statRequestKey.getKey() == null)
        || statRequest.getKeys().stream().allMatch(statRequestKey -> statRequestKey.getFilters() == null
        || statRequestKey.getFilters().isEmpty() || statRequestKey.getFilters().contains(null))
        || statRequest.getValues() == null
        || statRequest.getValues().isEmpty()
        || statRequest.getValues().stream().anyMatch(statRequestValue -> statRequestValue.getAggregations() == null
        || statRequestValue.getValue() == null
        || statRequestValue.getAggregations().isEmpty()
        || statRequestValue.getAggregations().contains(null)
        || statRequestValue.getAggregations().stream().anyMatch(func -> !func.usedInApi))
    ) {
      respond400(exchange, Map.of("validationErrors", "request body is incorrect: provide keys and values description"));
      return;
    }

    // convert keys data to more convenient format
    final Map<StatValue, Set<StatValueAggFunction>> statValueToAggFunctions = convertToValuesMap(statRequest.getValues());
    if (statValueToAggFunctions.isEmpty()) {
      exchange.sendResponseHeaders(204, -1);
      return;
    }

    // convert values data to more convenient format
    final Map<StatKey, Collection<String>> statKeyToFilters = convertToKeysMap(statRequest.getKeys());
    if (statKeyToFilters.isEmpty() || statKeyToFilters.values().stream().allMatch(Collection::isEmpty)) {
      exchange.sendResponseHeaders(204, -1);
      return;
    }

    // map to collect raw data both from the database and other backs:
    // season -> team -> player -> time -> record
    Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> rawDataCollector = new HashMap<>();

    // call another backs
    CompletableFuture<List<CacheRecord>> res = serviceCallUtil.callServices(statKeyToFilters);

    // load from database
    try {
      // merge the data from the db
      mergeResultToCollector(DatabaseUtil.loadFromDatabase(statKeyToFilters, statValueToAggFunctions.keySet()), rawDataCollector);
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.sendResponseHeaders(204, -1);
      return;
    }

    // merge the data from another backs
    mergeResultToCollector(res.join(), rawDataCollector);

    // calc statistics
    List<StatRecord> stats = calcStats(rawDataCollector, statValueToAggFunctions);
    if (stats.isEmpty()) {
      exchange.sendResponseHeaders(204, -1);
    } else {
      final byte[] responseBody = mapper.writeValueAsBytes(stats);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, responseBody.length);
      try (OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(responseBody);
      }
    }
  }
}
