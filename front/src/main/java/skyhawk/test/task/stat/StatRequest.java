package skyhawk.test.task.stat;

import lombok.Data;

import java.util.Set;

@Data
public class StatRequest {

  private String season;
  private StatPer per;
  private Set<StatValue> values;
}
