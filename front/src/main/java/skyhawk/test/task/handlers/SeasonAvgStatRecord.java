package skyhawk.test.task.handlers;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import skyhawk.test.task.stat.StatValue;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SeasonAvgStatRecord(
    String team,
    String player,
    Map<StatValue, Number> values
) {
}
