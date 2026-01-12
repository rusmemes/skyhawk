package skyhawk.test.task.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.service.RuntimeStore;
import skyhawk.test.task.service.ServiceDiscovery;

public class ServiceCallUtil {

  private static final Logger log = LoggerFactory.getLogger(ServiceCallUtil.class);

  private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.getInstance();
  private final ObjectMapper mapper = new ObjectMapper();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;
  private final HttpClient httpClient = HttpClient.newBuilder()
      .executor(Executors.newVirtualThreadPerTaskExecutor())
      .build();

  public CompletableFuture<List<CacheRecord>> callServices(String season) {

    final List<URI> serviceList = serviceDiscovery.getServiceList();
    if (serviceList.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }

    CompletableFuture<Void> res = CompletableFuture.completedFuture(null);
    for (URI uri : serviceList) {

      try {
        uri = new URI(uri + "/stat-copy");
      } catch (URISyntaxException e) {
        log.error("URISyntaxException", e);
        continue;
      }

      CompletableFuture<Void> voidCompletableFuture = callService(season.getBytes(StandardCharsets.UTF_8), uri)
          .thenAccept(list -> list.forEach(runtimeStore::log));

      res = res.thenCombine(voidCompletableFuture, (_, _) -> null);
    }

    return res.thenApply(_ -> runtimeStore.copy(season));
  }

  private CompletableFuture<List<CacheRecord>> callService(byte[] req, URI finalUri) {

    final CompletableFuture<HttpResponse<byte[]>> response;
    try {
      response = httpClient.sendAsync(
          HttpRequest.newBuilder()
              .uri(finalUri)
              .POST(HttpRequest.BodyPublishers.ofByteArray(req))
              .build(),
          HttpResponse.BodyHandlers.ofByteArray()
      );
    } catch (Throwable e) {
      log.error("Error calling service", e);
      serviceDiscovery.reportBadURI(finalUri);
      return CompletableFuture.completedFuture(List.of());
    }

    return response.thenApply(
        r -> {
          final int statusCode = r.statusCode();
          if (statusCode != 200) {
            return List.of();
          }

          final byte[] body = r.body();

          try {
            return Arrays.asList(mapper.readValue(body, CacheRecord[].class));
          } catch (IOException e) {
            log.error("Error calling service", e);
            return List.of();
          }
        }
    );
  }
}
