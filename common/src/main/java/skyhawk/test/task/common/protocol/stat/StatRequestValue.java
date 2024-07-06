package skyhawk.test.task.common.protocol.stat;

import lombok.Data;

import java.util.Set;

@Data
public class StatRequestValue {

  private StatValue value;
  private Set<StatValueAggFunction> aggregations;
}
