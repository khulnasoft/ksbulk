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
package com.khulnasoft.oss.ksbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExecutionListener} that reports useful metrics about executed statements. It relies on
 * a delegate {@link MetricsCollectingExecutionListener} as its source of metrics.
 *
 * <p>When reporting statements, batch statements and read statements count for just one event.
 */
public class StatementsReportingExecutionListener extends AbstractMetricsReportingExecutionListener
    implements ExecutionListener {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StatementsReportingExecutionListener.class);

  private static final LogSink DEFAULT_SINK =
      LogSink.buildFrom(LOGGER::isInfoEnabled, LOGGER::info);

  private static final MetricFilter METRIC_FILTER =
      (name, metric) -> name.startsWith("executor/statements/");

  private static final String REPORTER_NAME = "bulk-execution-statements-reporter";

  /**
   * Creates a new builder for this class.
   *
   * @return a new builder.
   */
  public static AbstractMetricsReportingExecutionListenerBuilder<
          StatementsReportingExecutionListener>
      builder() {
    return new AbstractMetricsReportingExecutionListenerBuilder<
        StatementsReportingExecutionListener>() {
      @Override
      public StatementsReportingExecutionListener build() {
        LogSink s = sink == null ? DEFAULT_SINK : sink;
        if (scheduler == null) {
          return new StatementsReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, s);
        } else {
          return new StatementsReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, s, scheduler);
        }
      }
    };
  }

  private final long expectedTotal;
  private final String countMessage;
  private final String throughputMessage;
  private final String latencyMessage;
  private final Timer timer;
  private final Counter failed;
  private final Counter successful;
  private final Counter inFlight;
  @Nullable private final Meter sent;
  @Nullable private final Meter received;
  private final LogSink sink;

  /**
   * Creates a default instance of {@link StatementsReportingExecutionListener}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   */
  public StatementsReportingExecutionListener() {
    this(new MetricsCollectingExecutionListener(), SECONDS, MILLISECONDS, -1, DEFAULT_SINK);
  }

  /**
   * Creates an instance of {@link StatementsReportingExecutionListener} using the given {@linkplain
   * MetricsCollectingExecutionListener delegate}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   *
   * @param delegate the {@link StatementsReportingExecutionListener} to use as metrics source.
   */
  public StatementsReportingExecutionListener(MetricsCollectingExecutionListener delegate) {
    this(delegate, SECONDS, MILLISECONDS, -1, DEFAULT_SINK);
  }

  private StatementsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      LogSink sink) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit);
    this.expectedTotal = expectedTotal;
    this.sink = sink;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage =
        createThroughputMessageTemplate(
            delegate.getBytesSentMeter().isPresent()
                && delegate.getBytesReceivedMeter().isPresent());
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalStatementsTimer();
    successful = delegate.getSuccessfulStatementsCounter();
    failed = delegate.getFailedStatementsCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    sent = delegate.getBytesSentMeter().orElse(null);
    received = delegate.getBytesReceivedMeter().orElse(null);
  }

  StatementsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      LogSink sink,
      ScheduledExecutorService scheduler) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit, scheduler);
    this.expectedTotal = expectedTotal;
    this.sink = sink;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage =
        createThroughputMessageTemplate(
            delegate.getBytesSentMeter().isPresent()
                && delegate.getBytesReceivedMeter().isPresent());
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalStatementsTimer();
    successful = delegate.getSuccessfulStatementsCounter();
    failed = delegate.getFailedStatementsCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    sent = delegate.getBytesSentMeter().orElse(null);
    received = delegate.getBytesReceivedMeter().orElse(null);
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
    Snapshot snapshot = timer.getSnapshot();
    long total = timer.getCount();
    String durationUnit = getDurationUnit();
    String rateUnit = getRateUnit();
    if (expectedTotal < 0) {
      sink.accept(
          String.format(
              countMessage, total, successful.getCount(), failed.getCount(), inFlight.getCount()));
    } else {
      float achieved = (float) total / (float) expectedTotal * 100f;
      sink.accept(
          String.format(
              countMessage,
              total,
              successful.getCount(),
              failed.getCount(),
              inFlight.getCount(),
              achieved));
    }
    double throughput = timer.getMeanRate();
    if (sent != null && received != null) {
      double sizeSent = sent.getMeanRate();
      double sizeReceived = received.getMeanRate();
      sink.accept(
          String.format(
              throughputMessage,
              convertRate(throughput),
              rateUnit,
              convertRate(sizeSent / BYTES_PER_MB),
              rateUnit,
              convertRate(sizeReceived / BYTES_PER_MB),
              rateUnit,
              throughput == 0 ? 0 : (sizeSent / BYTES_PER_KB) / throughput,
              throughput == 0 ? 0 : (sizeReceived / BYTES_PER_KB) / throughput));
    } else {
      sink.accept(String.format(throughputMessage, convertRate(throughput), rateUnit));
    }
    sink.accept(
        String.format(
            latencyMessage,
            convertDuration(snapshot.getMean()),
            convertDuration(snapshot.get75thPercentile()),
            convertDuration(snapshot.get99thPercentile()),
            convertDuration(snapshot.get999thPercentile()),
            durationUnit));
  }

  private static String createCountMessageTemplate(long expectedTotal) {
    if (expectedTotal < 0) {
      return "Statements: "
          + "total: %,d, "
          + "successful: %,d, "
          + "failed: %,d, "
          + "in-flight: %,d";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      return "Statements: "
          + "total: %,"
          + numDigits
          + "d, "
          + "successful: %,"
          + numDigits
          + "d, "
          + "failed: %,d, "
          + "in-flight: %,d, "
          + "progression: %,.0f%%";
    }
  }

  private static String createThroughputMessageTemplate(boolean trackThroughputInBytes) {
    if (trackThroughputInBytes) {
      return "Throughput: "
          + "%,.0f stmts/%s, "
          + "%,.2f mb/%s sent, "
          + "%,.2f mb/%s received ("
          + "%,.3f kb/write, "
          + "%,.3f kb/read)";
    } else {
      return "Throughput: %,.0f stmts/%s";
    }
  }

  private static String createLatencyMessageTemplate() {
    return "Latencies: mean %,.2f, 75p %,.2f, 99p %,.2f, 999p %,.2f %s";
  }
}
