package skyhawk.test.task.common.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Log(
  String season,
  String team,
  String player,
  Integer points,
  Integer rebounds,
  Integer assists,
  Integer steals,
  Integer blocks,
  Integer fouls,
  Integer turnovers,
  Double minutesPlayed
) {
}
