package skyhawk.test.task.stat;

import skyhawk.test.task.common.protocol.Log;

import java.util.function.Function;

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
