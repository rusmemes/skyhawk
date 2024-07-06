package skyhawk.test.task.util;

import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.Log;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.common.protocol.stat.StatKey;
import skyhawk.test.task.common.protocol.stat.StatRequestKey;
import skyhawk.test.task.common.protocol.stat.StatRequestValue;
import skyhawk.test.task.common.protocol.stat.StatValue;
import skyhawk.test.task.common.protocol.stat.StatValueAggFunction;
import skyhawk.test.task.handlers.StatRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatUtils {

  public static void mergeResultToCollector(
      List<CacheRecord> statRequest, Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> merged
  ) {
    statRequest.forEach(cacheRecord -> merge(merged, cacheRecord));
  }

  private static void merge(
      Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> collector,
      CacheRecord cacheRecord
  ) {
    final Log log = cacheRecord.log();
    collector
        .computeIfAbsent(log.getSeason(), s -> new HashMap<>())
        .computeIfAbsent(log.getTeam(), t -> new HashMap<>())
        .computeIfAbsent(log.getPlayer(), p -> new HashMap<>())
        .put(cacheRecord.timeKey(), cacheRecord);
  }

  public static Map<StatKey, Collection<String>> convertToKeysMap(Set<StatRequestKey> keys) {
    if (keys.isEmpty()) {
      return Map.of();
    }
    final Map<StatKey, Collection<String>> res = new HashMap<>();
    for (StatRequestKey key : keys) {
      Set<String> filters = key.getFilters() == null ? Set.of() : key.getFilters();
      res.put(key.getKey(), filters);
    }
    return res;
  }

  public static Map<StatValue, Set<StatValueAggFunction>> convertToValuesMap(Set<StatRequestValue> values) {
    if (values.isEmpty()) {
      return Map.of();
    }
    final Map<StatValue, Set<StatValueAggFunction>> res = new HashMap<>();
    for (StatRequestValue value : values) {
      final Set<StatValueAggFunction> aggregations = value.getAggregations() == null ? Set.of() : value.getAggregations();
      res.put(value.getValue(), aggregations);
    }
    return res;
  }

  public static List<StatRecord> calcStats(
      Map<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> merged, Map<StatValue, Set<StatValueAggFunction>> values
  ) {
    Map<String, Map<String, Map<String, StatRecord>>> stats = new HashMap<>();

    final List<StatRecord> statRecords = new ArrayList<>();
    for (Map.Entry<String, Map<String, Map<String, Map<TimeKey, CacheRecord>>>> entry : merged.entrySet()) {
      final String season = entry.getKey();
      for (Map.Entry<String, Map<String, Map<TimeKey, CacheRecord>>> mapEntry : entry.getValue().entrySet()) {
        final String team = mapEntry.getKey();
        for (Map.Entry<String, Map<TimeKey, CacheRecord>> stringMapEntry : mapEntry.getValue().entrySet()) {
          final String player = stringMapEntry.getKey();
          for (Map.Entry<TimeKey, CacheRecord> timeKeyCacheRecordEntry : stringMapEntry.getValue().entrySet()) {
            final Log record = timeKeyCacheRecordEntry.getValue().log();
            stats
                .computeIfAbsent(season, s -> new HashMap<>())
                .computeIfAbsent(team, s -> new HashMap<>())
                .compute(player, (p, existing) -> {
                  if (existing == null) {
                    Map<StatValue, Map<StatValueAggFunction, Number>> aggregations = new HashMap<>();
                    aggregate(values, aggregations, record);
                    return new StatRecord(season, team, player, aggregations);
                  } else {
                    final Map<StatValue, Map<StatValueAggFunction, Number>> aggregations = existing.values();
                    aggregate(values, aggregations, record);
                    return existing;
                  }
                });

          }
        }
      }
    }

    for (Map.Entry<String, Map<String, Map<String, StatRecord>>> entry : stats.entrySet()) {
      for (Map.Entry<String, Map<String, StatRecord>> mapEntry : entry.getValue().entrySet()) {
        for (Map.Entry<String, StatRecord> recordEntry : mapEntry.getValue().entrySet()) {

          final StatRecord statRecord = recordEntry.getValue();

          for (Map.Entry<StatValue, Set<StatValueAggFunction>> setEntry : values.entrySet()) {
            final StatValue statValue = setEntry.getKey();
            final Set<StatValueAggFunction> aggFunctionSet = setEntry.getValue();
            if (aggFunctionSet.contains(StatValueAggFunction.avg)) {
              final Map<StatValueAggFunction, Number> map = statRecord.values().getOrDefault(statValue, Map.of());
              final Number total = map.get(StatValueAggFunction.total);
              if (total != null) {
                final Number sum = map.get(StatValueAggFunction.sum);
                map.put(StatValueAggFunction.avg, sum.doubleValue() / total.doubleValue());
                map.remove(StatValueAggFunction.total);
                if (!aggFunctionSet.contains(StatValueAggFunction.sum)) {
                  map.remove(StatValueAggFunction.sum);
                }
              }
            }

          }
          statRecords.add(statRecord);
        }
      }
    }

    return statRecords;
  }

  private static void aggregate(
      Map<StatValue, Set<StatValueAggFunction>> values,
      Map<StatValue, Map<StatValueAggFunction, Number>> aggregations,
      Log record
  ) {
    for (Map.Entry<StatValue, Set<StatValueAggFunction>> setEntry : values.entrySet()) {
      final StatValue statValue = setEntry.getKey();
      final Set<StatValueAggFunction> aggFunctionSet = setEntry.getValue();
      for (final StatValueAggFunction aggFunction : aggFunctionSet) {
        aggregate(aggregations, record, aggFunction, statValue, aggFunctionSet);
      }
    }
  }

  private static void aggregate(
      Map<StatValue, Map<StatValueAggFunction, Number>> aggregations,
      Log record,
      StatValueAggFunction aggFunction,
      StatValue statValue,
      Set<StatValueAggFunction> allAggFunctions
  ) {
    switch (aggFunction) {
      case sum -> aggregations
          .computeIfAbsent(statValue, s -> new HashMap<>())
          .compute(aggFunction, (agf, existingValue) -> resolveSum(existingValue, statValue.getter.apply(record)));
      case avg -> {
        aggregations
            .computeIfAbsent(statValue, s -> new HashMap<>())
            .compute(StatValueAggFunction.total, (agf, existingValue) -> resolveTotal(existingValue, statValue.getter.apply(record)));

        if (!allAggFunctions.contains(StatValueAggFunction.sum)) {
          aggregate(aggregations, record, StatValueAggFunction.sum, statValue, allAggFunctions);
        }
      }
      case min -> aggregations
          .computeIfAbsent(statValue, s -> new HashMap<>())
          .compute(aggFunction, (agf, existingValue) -> resolveMin(existingValue, statValue.getter.apply(record)));
      case max -> aggregations
          .computeIfAbsent(statValue, s -> new HashMap<>())
          .compute(aggFunction, (agf, existingValue) -> resolveMax(existingValue, statValue.getter.apply(record)));
      case total -> {
      }
    }
  }

  private static Number resolveMin(Number existingValue, Number newValue) {
    return newValue == null
        ? existingValue
        : existingValue == null
        ? newValue
        : existingValue.doubleValue() > newValue.doubleValue() ? newValue : existingValue;
  }

  private static Number resolveMax(Number existingValue, Number newValue) {
    return newValue == null
        ? existingValue
        : existingValue == null
        ? newValue
        : existingValue.doubleValue() > newValue.doubleValue() ? existingValue : newValue;
  }

  private static Number resolveTotal(Number existingValue, Number newValue) {
    return newValue == null
        ? existingValue
        : existingValue == null
        ? 1
        : existingValue.intValue() + 1;
  }

  private static Number resolveSum(Number existingValue, Number newValue) {
    return newValue == null
        ? existingValue
        : existingValue == null
        ? newValue
        : existingValue instanceof Double d
        ? d + newValue.doubleValue()
        : existingValue.intValue() + newValue.intValue();
  }
}
