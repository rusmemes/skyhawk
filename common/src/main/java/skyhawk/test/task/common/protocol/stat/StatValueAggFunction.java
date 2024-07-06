package skyhawk.test.task.common.protocol.stat;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum StatValueAggFunction {

  sum(true),
  avg(true),
  min(true),
  max(true),
  total(false);

  public final boolean usedInApi;
}
