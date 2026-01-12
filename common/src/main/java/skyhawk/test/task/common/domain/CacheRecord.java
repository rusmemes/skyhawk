package skyhawk.test.task.common.domain;

public record CacheRecord(Log log, TimeKey timeKey) {

  public static final String HEADER_SENDER = "sender";
}
