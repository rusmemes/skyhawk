package skyhawk.test.task.common.utils;

import java.util.ArrayList;
import java.util.List;
import skyhawk.test.task.common.protocol.Log;

public class LogRecordUtil {

  public static String getAggregationKey(Log log) {
    return prepareAggregationKeyPart(log.season()) +
      "_" + prepareAggregationKeyPart(log.team()) +
      "_" + prepareAggregationKeyPart(log.player());
  }

  private static String prepareAggregationKeyPart(String part) {
    if (part == null || part.isBlank()) {
      return "";
    }
    return part.strip().toUpperCase();
  }

  public static List<String> validate(Log log) {

    List<String> errors = null;

    String season = log.season();
    if (season == null || season.isBlank()) {
      (errors = new ArrayList<>(11)).add("Missing season");
    } else if (season.trim().chars().anyMatch(Character::isWhitespace)) {
      (errors = new ArrayList<>(11)).add("season value contains whitespaces");
    }

    String team = log.team();
    if (team == null || team.isBlank()) {
      (errors == null ? (errors = new ArrayList<>(10)) : errors).add("Missing team");
    } else if (team.trim().chars().anyMatch(Character::isWhitespace)) {
      (errors == null ? (errors = new ArrayList<>(10)) : errors).add("team value contains whitespaces");
    }

    String player = log.player();
    if (player == null || player.isBlank()) {
      (errors == null ? (errors = new ArrayList<>(9)) : errors).add("Missing player");
    } else if (player.trim().chars().anyMatch(Character::isWhitespace)) {
      (errors == null ? (errors = new ArrayList<>(9)) : errors).add("player value contains whitespaces");
    }

    if (log.points() == null
      && log.rebounds() == null
      && log.assists() == null
      && log.steals() == null
      && log.blocks() == null
      && log.fouls() == null
      && log.turnovers() == null
      && log.minutesPlayed() == null
    ) {
      (errors == null ? (errors = new ArrayList<>(8)) : errors).add("No values provided");
    } else {
      if (log.points() != null && log.points() < 1) {
        (errors == null ? (errors = new ArrayList<>(8)) : errors).add("Points value must be positive number");
      }
      if (log.rebounds() != null && log.rebounds() < 1) {
        (errors == null ? (errors = new ArrayList<>(7)) : errors).add("Rebounds value must be positive number");
      }
      if (log.assists() != null && log.assists() < 1) {
        (errors == null ? (errors = new ArrayList<>(6)) : errors).add("Assists value must be positive number");
      }
      if (log.steals() != null && log.steals() < 1) {
        (errors == null ? (errors = new ArrayList<>(5)) : errors).add("Steals value must be positive number");
      }
      if (log.blocks() != null && log.blocks() < 1) {
        (errors == null ? (errors = new ArrayList<>(4)) : errors).add("Blocks value must be positive number");
      }
      if (log.fouls() != null && (log.fouls() < 1 || log.fouls() > 6)) {
        (errors == null ? (errors = new ArrayList<>(3)) : errors).add("Fouls value must be positive number, max is 6");
      }
      if (log.turnovers() != null && log.turnovers() < 1) {
        (errors == null ? (errors = new ArrayList<>(2)) : errors).add("Turnovers value must be positive number");
      }
      if (log.minutesPlayed() != null && (log.minutesPlayed() < 0.0 || log.minutesPlayed() > 48.0)) {
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
