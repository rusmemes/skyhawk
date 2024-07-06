package skyhawk.test.task.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.service.discovery.ServiceDiscovery;
import skyhawk.test.task.runtime.store.RuntimeStore;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceCallUtil {

  private final ServiceDiscovery serviceDiscovery = ServiceDiscovery.getInstance();
  private final ObjectMapper mapper = new ObjectMapper();
  private final RuntimeStore runtimeStore = RuntimeStore.INSTANCE;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final HttpClient httpClient = HttpClient.newHttpClient();

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

      final URI finalUri = uri;
      CompletableFuture<List<CacheRecord>> f = CompletableFuture.supplyAsync(
          () -> callService(season.getBytes(StandardCharsets.UTF_8), finalUri),
          executorService
      );

      res = res.thenCombine(f, (n, r) -> {
        r.forEach(runtimeStore::log);
        return null;
      });
    }

    return res.thenApply(n -> runtimeStore.copy(season));
  }

  private List<CacheRecord> callService(byte[] req, URI finalUri) {
    final HttpResponse<byte[]> response;
    try {
      response = httpClient.send(
          HttpRequest.newBuilder()
              .uri(finalUri)
              .POST(HttpRequest.BodyPublishers.ofByteArray(req))
              .build(),
          HttpResponse.BodyHandlers.ofByteArray()
      );
    } catch (Throwable e) {
      log.error("Error calling service", e);
      serviceDiscovery.reportBadURI(finalUri);
      return List.of();
    }

    final int statusCode = response.statusCode();
    if (statusCode != 200) {
      return List.of();
    }

    final byte[] body = response.body();

    try {
      return Arrays.asList(mapper.readValue(body, CacheRecord[].class));
    } catch (IOException e) {
      log.error("Error calling service", e);
      return List.of();
    }
  }
}
