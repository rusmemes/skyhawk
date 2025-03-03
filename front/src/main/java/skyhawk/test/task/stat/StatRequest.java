package skyhawk.test.task.stat;

import java.util.Set;

public record StatRequest(String season, StatPer per, Set<StatValue> values) {
}
