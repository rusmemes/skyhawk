package skyhawk.test.task.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class Env {

  public static final String instanceId = UUID.randomUUID().toString();

  public static int getPort() {
    return getIntOrDefault("port", 8080);
  }

  public static int getSocketBacklog() {
    return getIntOrDefault("socket.backlog", 0);
  }

  public static String getDbUrl() {
    return getRequiredString("db.url");
  }

  public static String getDbUsername() {
    return getRequiredString("db.username");
  }

  public static String getDbPassword() {
    return getRequiredString("db.password");
  }

  public static String getServiceDiscoverySelfUrl() {
    final String url = getRequiredString("service.discovery.self.url");
    if (url.equals("docker.host")) {
      return "http://" + getDockerHost() + ":" + getPort();
    }

    return url;
  }

  private static String getDockerHost() {
    try {
      final InetAddress localHost = InetAddress.getLocalHost();
      String dockerHost = localHost.getHostName();
      if (dockerHost == null || dockerHost.isEmpty()) {
        dockerHost = localHost.getCanonicalHostName();
      }
      if (dockerHost == null || dockerHost.isEmpty()) {
        throw new IllegalStateException("unable to determine docker host");
      }
      return dockerHost;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isServiceDiscoveryHeartbeatEnabled() {
    return getBooleanOrDefault("service.discovery.heartbeat.enabled", false);
  }

  public static long getServiceDiscoveryExpirationTime() {
    return getLongOrDefault("service.discovery.expiration.time.ms", 5000L);
  }

  public static long getServiceDiscoveryHeartbeatIntervalMs() {
    return getLongOrDefault("service.discovery.heartbeat.interval.ms", 1000L);
  }

  public static String getKafkaBootstrapServers() {
    return getRequiredString("kafka.bootstrap.servers");
  }

  public static String getKafkaTopicMain() {
    return getRequiredString("kafka.topic.main");
  }

  public static String getKafkaTopicRemoval() {
    return getRequiredString("kafka.topic.removal");
  }

  public static String getKafkaGroupId() {
    return getRequiredString("kafka.group.id");
  }

  private static String getRequiredString(String name) {
    return Objects.requireNonNull(System.getProperty(name), name + " property not set");
  }

  private static long getLongOrDefault(String name, long defaultValue) {
    final String property = System.getProperty(name);
    if (property != null) {
      try {
        return Long.parseLong(property);
      } catch (Throwable e) {
        log.error("unable to parse {}", property, e);
      }
    }
    System.err.println(name + " property not set, " + defaultValue + " will be used as default");
    return defaultValue;
  }

  private static int getIntOrDefault(String name, int defaultValue) {
    final String property = System.getProperty(name);
    if (property != null) {
      try {
        return Integer.parseInt(property);
      } catch (Throwable e) {
        log.error("unable to parse {}", property, e);
      }
    }
    System.err.println(name + " property not set, " + defaultValue + " will be used as default");
    return defaultValue;
  }

  private static boolean getBooleanOrDefault(String name, boolean defaultValue) {
    final String property = System.getProperty(name);
    if (property != null) {
      try {
        return Boolean.parseBoolean(property);
      } catch (Throwable e) {
        log.error("unable to parse {}", property, e);
      }
    }
    System.err.println(name + " property not set, " + defaultValue + " will be used as default");
    return defaultValue;
  }
}
