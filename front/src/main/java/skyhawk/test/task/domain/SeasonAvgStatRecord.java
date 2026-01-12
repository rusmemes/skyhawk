package skyhawk.test.task.domain;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SeasonAvgStatRecord(
    String team,
    String player,
    Map<StatValue, Number> values
) {
}
