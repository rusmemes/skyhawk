package skyhawk.test.task.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyhawk.test.task.common.domain.CacheRecord;
import skyhawk.test.task.common.domain.Log;
import skyhawk.test.task.common.domain.TimeKey;

public class RuntimeStore {

  private static final Logger log = LoggerFactory.getLogger(RuntimeStore.class);

  public static final RuntimeStore INSTANCE = new RuntimeStore();

  // season -> team -> player -> time -> log record
  private final Map<String, Map<String, Map<String, SortedMap<TimeKey, CacheRecord>>>> cache = new ConcurrentHashMap<>();

  private RuntimeStore() {
  }

  public void log(CacheRecord cacheRecord) {
    final Log log = cacheRecord.log();
    cache
        .computeIfAbsent(log.season(), _ -> new ConcurrentHashMap<>())
        .computeIfAbsent(log.team(), _ -> new ConcurrentHashMap<>())
        .computeIfAbsent(log.player(), _ -> new ConcurrentSkipListMap<>())
        .put(cacheRecord.timeKey(), cacheRecord);
  }

  public void remove(CacheRecord cacheRecord) {

    final Log logged = cacheRecord.log();
    final SortedMap<TimeKey, CacheRecord> map = cache
        .getOrDefault(logged.season(), Map.of())
        .getOrDefault(logged.team(), Map.of())
        .get(logged.player());

    if (map != null && !map.isEmpty()) {
      TimeKey currentEarliest;
      try {
        currentEarliest = map.firstKey();
      } catch (Throwable e) {
        log.error("Failed to read earliest time key", e);
        return;
      }
      final TimeKey timeKeyToRemoveUpTo = cacheRecord.timeKey();
      while (currentEarliest != null && timeKeyToRemoveUpTo.compareTo(currentEarliest) > -1) {
        map.remove(currentEarliest);
        if (map.isEmpty()) currentEarliest = null;
        else try {
          currentEarliest = map.firstKey();
        } catch (Throwable e) {
          log.error("Failed to read earliest time key", e);
          currentEarliest = null;
        }
      }
    }
  }

  public List<CacheRecord> copy(String season) {

    if (cache.isEmpty()) {
      return List.of();
    }

    final Map<String, Map<String, SortedMap<TimeKey, CacheRecord>>> teamData = cache.getOrDefault(season, Map.of());
    if (teamData.isEmpty()) {
      return List.of();
    }

    List<CacheRecord> res = new ArrayList<>();

    teamData.forEach((_, playersData) ->
        playersData.forEach((_, records) ->
            res.addAll(records.values())
        )
    );

    return res;
  }
}
