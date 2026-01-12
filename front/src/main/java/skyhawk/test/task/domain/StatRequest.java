package skyhawk.test.task.domain;

import java.util.Set;

public record StatRequest(String season, StatPer per, Set<StatValue> values) {
}
