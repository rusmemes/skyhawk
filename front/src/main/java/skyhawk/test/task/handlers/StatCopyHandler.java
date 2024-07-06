package skyhawk.test.task.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.stat.StatKey;
import skyhawk.test.task.runtime.store.RuntimeStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatCopyHandler implements HttpHandler {

  private final ObjectMapper mapper = new ObjectMapper();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    final byte[] bytes = exchange.getRequestBody().readAllBytes();
    final Map<?, ?> statRequest = mapper.readValue(bytes, Map.class);

    final Map<StatKey, Collection<String>> statKeyToFilters = new HashMap<>();

    for (Map.Entry<?, ?> entry : statRequest.entrySet()) {
      final StatKey statKey = StatKey.valueOf(entry.getKey().toString());
      final Object value = entry.getValue();
      statKeyToFilters.put(
          statKey,
          value instanceof Collection<?> coll
              ? coll.stream().map(String.class::cast).collect(Collectors.toSet())
              : List.of()
      );
    }

    final List<CacheRecord> collected = runtimeStore.copy(statKeyToFilters);

    if (collected.isEmpty()) {
      exchange.sendResponseHeaders(204, -1);
    } else {
      final byte[] responseBody = mapper.writeValueAsBytes(collected);
      exchange.sendResponseHeaders(200, responseBody.length);
      try (OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(responseBody);
      }
    }
  }
}
