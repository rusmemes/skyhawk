package skyhawk.test.task.common.protocol.stat;

import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class StatKeys {
  private Map<StatKey, Set<String>> keys;
}
