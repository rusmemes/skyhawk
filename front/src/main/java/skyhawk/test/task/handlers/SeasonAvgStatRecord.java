package skyhawk.test.task.handlers;

import com.fasterxml.jackson.annotation.JsonInclude;
import skyhawk.test.task.stat.StatValue;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SeasonAvgStatRecord(
    String team, String player, Map<StatValue, Number> values
) {
}
