package skyhawk.test.task.common.protocol;

public record TimeKey(long t1, long t2) implements Comparable<TimeKey> {
  @Override
  public int compareTo(TimeKey that) {
    int res = Long.compare(this.t1, that.t1);
    return res == 0 ? Long.compare(this.t2, that.t2) : res;
  }

  public static TimeKey ofCurrentTime() {
    return new TimeKey(System.currentTimeMillis(), System.nanoTime());
  }
}
