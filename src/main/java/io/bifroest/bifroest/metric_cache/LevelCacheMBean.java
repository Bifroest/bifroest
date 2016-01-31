package io.bifroest.bifroest.metric_cache;

import java.util.Map;

public interface LevelCacheMBean {
    Map<String, String> getCacheLineAge();
    Map<String, String> getContents();
}
