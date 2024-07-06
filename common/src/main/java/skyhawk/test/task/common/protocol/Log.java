package skyhawk.test.task.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Log {

  private String season;
  private String team;
  private String player;
  private Integer points;
  private Integer rebounds;
  private Integer assists;
  private Integer steals;
  private Integer blocks;
  private Integer fouls;
  private Integer turnovers;
  private Double minutesPlayed;

  @JsonIgnore
  public String getAggregationKey() {
    return prepareAggregationKeyPart(season) + "_" + prepareAggregationKeyPart(team) + "_" + prepareAggregationKeyPart(player);
  }

  public static String prepareAggregationKeyPart(String part) {
    if (part == null || part.isBlank()) {
      return "";
    }
    return part.strip().toUpperCase();
  }

  public List<String> validate() {
    List<String> errors = null;
    if (season == null || season.isBlank()) {
      (errors = new ArrayList<>(11)).add("Missing season");
    }
    if (team == null || team.isBlank()) {
      (errors == null ? (errors = new ArrayList<>(10)) : errors).add("Missing team");
    }
    if (player == null || player.isBlank()) {
      (errors == null ? (errors = new ArrayList<>(9)) : errors).add("Missing player");
    }
    if (points == null
        && rebounds == null
        && assists == null
        && steals == null
        && blocks == null
        && fouls == null
        && turnovers == null
        && minutesPlayed == null
    ) {
      (errors == null ? (errors = new ArrayList<>(8)) : errors).add("No values provided");
    } else {
      if (points != null && points < 1) {
        (errors == null ? (errors = new ArrayList<>(8)) : errors).add("Points value must be positive number");
      }
      if (rebounds != null && rebounds < 1) {
        (errors == null ? (errors = new ArrayList<>(7)) : errors).add("Rebounds value must be positive number");
      }
      if (assists != null && assists < 1) {
        (errors == null ? (errors = new ArrayList<>(6)) : errors).add("Assists value must be positive number");
      }
      if (steals != null && steals < 1) {
        (errors == null ? (errors = new ArrayList<>(5)) : errors).add("Steals value must be positive number");
      }
      if (blocks != null && blocks < 1) {
        (errors == null ? (errors = new ArrayList<>(4)) : errors).add("Blocks value must be positive number");
      }
      if (fouls != null && (fouls < 1 || fouls > 6)) {
        (errors == null ? (errors = new ArrayList<>(3)) : errors).add("Fouls value must be positive number, max is 6");
      }
      if (turnovers != null && turnovers < 1) {
        (errors == null ? (errors = new ArrayList<>(2)) : errors).add("Turnovers value must be positive number");
      }
      if (minutesPlayed != null && (minutesPlayed < 0.0 || minutesPlayed > 48.0)) {
        final String error = "Minutes played value must be positive number between 0 and 48";
        if (errors == null) {
          errors = List.of(error);
        } else {
          errors.add(error);
        }
      }
    }
    return errors == null ? List.of() : errors;
  }
}
