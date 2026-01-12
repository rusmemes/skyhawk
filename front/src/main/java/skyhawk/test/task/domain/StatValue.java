package skyhawk.test.task.domain;

import java.util.function.Function;
import skyhawk.test.task.common.domain.Log;

public enum StatValue {

  points(Log::points),
  rebounds(Log::rebounds),
  assists(Log::assists),
  steals(Log::steals),
  blocks(Log::blocks),
  fouls(Log::fouls),
  turnovers(Log::turnovers),
  minutesPlayed(Log::minutesPlayed);

  public final Function<Log, ? extends Number> getter;

  StatValue(Function<Log, ? extends Number> getter) {
    this.getter = getter;
  }
}
