package skyhawk.test.task.common.protocol.stat;

import skyhawk.test.task.common.protocol.Log;

import java.util.function.Function;

public enum StatValue {

  points(Log::getPoints),
  rebounds(Log::getRebounds),
  assists(Log::getAssists),
  steals(Log::getSteals),
  blocks(Log::getBlocks),
  fouls(Log::getFouls),
  turnovers(Log::getTurnovers),
  minutesPlayed(Log::getMinutesPlayed);

  public final Function<Log, ? extends Number> getter;

  StatValue(Function<Log, ? extends Number> getter) {
    this.getter = getter;
  }
}
