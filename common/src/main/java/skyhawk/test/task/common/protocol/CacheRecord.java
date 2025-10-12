package skyhawk.test.task.common.protocol;

public record CacheRecord(Log log, TimeKey timeKey) {

  public static final String HEADER_SENDER = "sender";
}
