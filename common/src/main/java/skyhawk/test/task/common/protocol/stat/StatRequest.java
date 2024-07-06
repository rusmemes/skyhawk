package skyhawk.test.task.common.protocol.stat;

import lombok.Data;

import java.util.Set;

@Data
public class StatRequest {

  private Set<StatRequestKey> keys;
  private Set<StatRequestValue> values;
}
