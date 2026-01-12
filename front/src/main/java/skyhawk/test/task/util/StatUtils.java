package skyhawk.test.task.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.common.domain.Log;
import skyhawk.test.task.common.domain.TimeKey;
import skyhawk.test.task.domain.SeasonAvgStatRecord;
import skyhawk.test.task.domain.StatPer;
import skyhawk.test.task.domain.StatValue;
import skyhawk.test.task.domain.StatValueAggFunction;

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
        .computeIfAbsent(log.season(), _ -> new HashMap<>())
        .computeIfAbsent(log.team(), _ -> new HashMap<>())
        .computeIfAbsent(log.player(), _ -> new HashMap<>())
        .put(cacheRecord.timeKey(), cacheRecord);
  }

  public static Map<String, Map<StatValue, Number>> calcStats(
      // team -> player -> time -> record
      Map<String, Map<String, Map<TimeKey, CacheRecord>>> collectedData,
      Set<StatValue> values,
      StatPer per
  ) {
    final ArrayList<SeasonAvgStatRecord> statRecords = new ArrayList<>();
    collectedData.forEach((team, v) ->
        v.forEach((player, v1) ->
            v1.values().forEach(record -> {

              final HashMap<StatValue, Number> map = new HashMap<>();
              for (StatValue value : values) {
                final Number number = value.getter.apply(record.log());
                if (number != null) map.put(value, number);
              }

              statRecords.add(new SeasonAvgStatRecord(
                  team, player, map
              ));
            })
        )
    );

    final HashMap<String, Map<StatValue, Map<StatValueAggFunction, Number>>> aggKey2Value2Func2Number = new HashMap<>();
    statRecords.forEach(statRecord -> {

      String aggKey = switch (per) {
        case team -> statRecord.team();
        case player -> statRecord.player();
      };

      final Map<StatValue, Map<StatValueAggFunction, Number>> statValue2Func2Number = aggKey2Value2Func2Number
          .computeIfAbsent(aggKey, _ -> new HashMap<>());

      for (StatValue value : values) {
        final Map<StatValue, Number> map = statRecord.values();
        final Number number = map.get(value);

        if (number != null) {

          final Map<StatValueAggFunction, Number> aggFuncToNumber = statValue2Func2Number
              .computeIfAbsent(value, _ -> new HashMap<>());

          aggFuncToNumber.compute(
              StatValueAggFunction.total,
              (_, oldValue) -> oldValue == null ? 1 : oldValue.intValue() + 1
          );

          aggFuncToNumber.compute(
              StatValueAggFunction.sum,
              (_, oldValue) -> oldValue == null
                  ? number
                  : oldValue instanceof Double d
                  ? d + number.doubleValue()
                  : oldValue.intValue() + number.intValue()
          );
        }
      }
    });

    final HashMap<String, Map<StatValue, Number>> res = new HashMap<>();

    aggKey2Value2Func2Number.forEach((aggKey, v) ->
        v.forEach((statValue, funcToNumber) -> {
          final Map<StatValue, Number> statValueNumberMap = res.computeIfAbsent(aggKey, _ -> new HashMap<>());
          statValueNumberMap.put(
              statValue,
              funcToNumber.get(StatValueAggFunction.sum).doubleValue() / funcToNumber.get(StatValueAggFunction.total).doubleValue()
          );
        })
    );

    return res;
  }
}
