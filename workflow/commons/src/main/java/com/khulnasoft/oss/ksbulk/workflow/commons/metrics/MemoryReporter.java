/*
 * Copyright KhulnaSoft, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.khulnasoft.oss.ksbulk.workflow.commons.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.khulnasoft.oss.ksbulk.executor.api.listener.LogSink;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;

public class MemoryReporter extends ScheduledReporter {

  private static final String MSG =
      "Memory usage: used: %,d MB, free: %,d MB, allocated: %,d MB, available: %,d MB, "
          + "total gc count: %,d, total gc time: %,d ms";

  private final LogSink sink;

  MemoryReporter(MetricRegistry registry, LogSink sink, ScheduledExecutorService scheduler) {
    super(registry, "memory-reporter", createFilter(), SECONDS, MILLISECONDS, scheduler);
    this.sink = sink;
  }

  private static MetricFilter createFilter() {
    return (name, metric) ->
        name.equals("memory/used")
            || name.equals("memory/free")
            || name.equals("memory/allocated")
            || name.equals("memory/available")
            || name.equals("memory/gc_count")
            || name.equals("memory/gc_time");
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    if (!sink.isEnabled()) {
      return;
    }
    Gauge<?> freeMemoryGauge = gauges.get("memory/free");
    Gauge<?> allocatedMemoryGauge = gauges.get("memory/allocated");
    Gauge<?> usedMemoryGauge = gauges.get("memory/used");
    Gauge<?> availableMemoryGauge = gauges.get("memory/available");
    Gauge<?> gcCountGauge = gauges.get("memory/gc_count");
    Gauge<?> gcTimeGauge = gauges.get("memory/gc_time");
    long usedMemory = (Long) usedMemoryGauge.getValue();
    long freeMemory = (Long) freeMemoryGauge.getValue();
    long allocatedMemory = (Long) allocatedMemoryGauge.getValue();
    long availableMemory = (Long) availableMemoryGauge.getValue();
    long gcCount = (Long) gcCountGauge.getValue();
    long gcTime = (Long) gcTimeGauge.getValue();
    sink.accept(
        String.format(
            MSG, usedMemory, freeMemory, allocatedMemory, availableMemory, gcCount, gcTime));
  }
}
