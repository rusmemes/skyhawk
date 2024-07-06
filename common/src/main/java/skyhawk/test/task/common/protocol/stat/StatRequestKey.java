package skyhawk.test.task.common.protocol.stat;

import lombok.Data;

import java.util.Set;

@Data
public class StatRequestKey {
  private StatKey key;
  private Set<String> filters;
}
