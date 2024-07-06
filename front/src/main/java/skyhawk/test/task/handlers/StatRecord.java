package skyhawk.test.task.handlers;

import com.fasterxml.jackson.annotation.JsonInclude;
import skyhawk.test.task.common.protocol.stat.StatValue;
import skyhawk.test.task.common.protocol.stat.StatValueAggFunction;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record StatRecord(
    String season, String team, String player, Map<StatValue, Map<StatValueAggFunction, Number>> values
) {
}
