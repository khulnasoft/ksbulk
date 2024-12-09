/*
 * Copyright KhulnaSoft, Inc.
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

import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.event.Level.DEBUG;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.khulnasoft.oss.ksbulk.executor.api.listener.LogSink;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(LogInterceptingExtension.class)
class RecordReporterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReporter.class);

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(
      @LogCapture(value = RecordReporter.class, level = DEBUG) LogInterceptor interceptor) {
    Counter totalCounter = registry.counter("records/total");
    Counter failedCounter = registry.counter("records/failed");
    LogSink sink = LogSink.buildFrom(LOGGER::isDebugEnabled, LOGGER::debug);
    RecordReporter reporter =
        new RecordReporter(
            registry, sink, SECONDS, Executors.newSingleThreadScheduledExecutor(), -1);
    reporter.report();
    assertThat(interceptor).hasMessageContaining("Records: total: 0, successful: 0, failed: 0");
    totalCounter.inc(3);
    failedCounter.inc();
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor).hasMessageContaining("Records: total: 3, successful: 2, failed: 1");
  }

  @Test
  void should_report_batches_with_expected_total(
      @LogCapture(value = RecordReporter.class, level = DEBUG) LogInterceptor interceptor) {
    Counter totalCounter = registry.counter("records/total");
    Counter failedCounter = registry.counter("records/failed");
    LogSink sink = LogSink.buildFrom(LOGGER::isDebugEnabled, LOGGER::debug);
    RecordReporter reporter =
        new RecordReporter(
            registry, sink, SECONDS, Executors.newSingleThreadScheduledExecutor(), 3);
    reporter.report();
    assertThat(interceptor)
        .hasMessageContaining("Records: total: 0, successful: 0, failed: 0, progression: 0%");
    totalCounter.inc(3);
    failedCounter.inc();
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor)
        .hasMessageContaining("Records: total: 3, successful: 2, failed: 1, progression: 100%");
  }
}
