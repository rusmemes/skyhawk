package skyhawk.test.task.runtime.store;

import skyhawk.test.task.common.protocol.CacheRecord;
import skyhawk.test.task.common.protocol.Log;
import skyhawk.test.task.common.protocol.TimeKey;
import skyhawk.test.task.common.protocol.stat.StatKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;


public class RuntimeStore {

  public static final RuntimeStore INSTANCE = new RuntimeStore();

  // season -> team -> player -> time -> log record
  private final Map<String, Map<String, Map<String, SortedMap<TimeKey, CacheRecord>>>> cache = new ConcurrentHashMap<>();

  private RuntimeStore() {
  }

  public void log(CacheRecord cacheRecord) {
    final Log log = cacheRecord.log();
    cache
        .computeIfAbsent(log.getSeason(), s -> new ConcurrentHashMap<>())
        .computeIfAbsent(log.getTeam(), t -> new ConcurrentHashMap<>())
        .computeIfAbsent(log.getPlayer(), p -> new ConcurrentSkipListMap<>())
        .put(cacheRecord.timeKey(), cacheRecord);
  }

  public void remove(CacheRecord cacheRecord) {

    final Log log = cacheRecord.log();
    final SortedMap<TimeKey, CacheRecord> map = cache
        .getOrDefault(log.getSeason(), Map.of())
        .getOrDefault(log.getTeam(), Map.of())
        .get(log.getPlayer());

    if (map != null && !map.isEmpty()) {
      final TimeKey timeKeyToRemoveUpTo = cacheRecord.timeKey();
      TimeKey currentEarliest = map.firstKey();
      while (currentEarliest != null && timeKeyToRemoveUpTo.compareTo(currentEarliest) > -1) {
        map.remove(currentEarliest);
        currentEarliest = map.isEmpty() ? null : map.firstKey();
      }
    }
  }

  public List<CacheRecord> copy(
      Map<StatKey, Collection<String>> keysInfo
  ) {
    if (cache.isEmpty()) {
      return List.of();
    }

    List<CacheRecord> res = new ArrayList<>();

    final Collection<String> seasonFilters = keysInfo.getOrDefault(StatKey.season, cache.keySet());
    for (String seasonFilter : seasonFilters) {
      final Map<String, Map<String, SortedMap<TimeKey, CacheRecord>>> teamData = cache.getOrDefault(seasonFilter, Map.of());
      if (teamData.isEmpty()) {
        continue;
      }
      final Collection<String> teamFilters = keysInfo.getOrDefault(StatKey.team, teamData.keySet());
      for (String teamFilter : teamFilters) {
        final Map<String, SortedMap<TimeKey, CacheRecord>> playerData = teamData.getOrDefault(teamFilter, Map.of());
        if (playerData.isEmpty()) {
          continue;
        }
        final Collection<String> playerFilters = keysInfo.getOrDefault(StatKey.player, playerData.keySet());
        for (String playerFilter : playerFilters) {
          final SortedMap<TimeKey, CacheRecord> records = playerData.get(playerFilter);
          if (records != null && !records.isEmpty()) {
            res.addAll(records.values());
          }
        }
      }
    }

    return res;
  }
}
