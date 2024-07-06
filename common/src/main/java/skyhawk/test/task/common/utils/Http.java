package skyhawk.test.task.common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;

public class Http {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void respond400(HttpExchange exchange, Object response) throws IOException {
    byte[] bytes = MAPPER.writeValueAsBytes(response);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(400, bytes.length);
    try (OutputStream outputStream = exchange.getResponseBody()) {
      outputStream.write(bytes);
    }
  }
}
