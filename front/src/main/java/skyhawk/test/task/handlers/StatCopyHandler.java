package skyhawk.test.task.handlers;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.service.RuntimeStore;

public class StatCopyHandler implements HttpHandler {

  private final ObjectMapper mapper = new ObjectMapper();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    final byte[] bytes = exchange.getRequestBody().readAllBytes();
    String season = new String(bytes, StandardCharsets.UTF_8);

    final List<CacheRecord> collected = runtimeStore.copy(season);

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
