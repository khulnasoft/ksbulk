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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.khulnasoft.oss.driver.api.core.ProtocolVersion;
import com.khulnasoft.oss.driver.api.core.cql.BatchStatement;
import com.khulnasoft.oss.driver.api.core.cql.ColumnDefinitions;
import com.khulnasoft.oss.driver.api.core.cql.DefaultBatchType;
import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.oss.driver.api.core.cql.SimpleStatement;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.khulnasoft.oss.ksbulk.executor.api.exception.BulkExecutionException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsCollectingExecutionListenerTest {

  private final Statement successfulRead = SimpleStatement.newInstance("irrelevant", 42);
  private final Statement failedRead = SimpleStatement.newInstance("irrelevant", 42);

  private final Statement successfulWrite =
      BatchStatement.newInstance(
          DefaultBatchType.UNLOGGED,
          SimpleStatement.newInstance("irrelevant", 42),
          SimpleStatement.newInstance("irrelevant", 42));
  private final Statement failedWrite =
      BatchStatement.newInstance(
          DefaultBatchType.UNLOGGED,
          SimpleStatement.newInstance("irrelevant", 42),
          SimpleStatement.newInstance("irrelevant", 42));

  private final Row row = mock(Row.class);

  @BeforeEach
  void setUp() {
    ColumnDefinitions variables = mock(ColumnDefinitions.class);
    when(variables.size()).thenReturn(1);
    when(row.getBytesUnsafe(0)).thenReturn(ByteBuffer.wrap(new byte[] {0, 0, 0, 42}));
    when(row.getColumnDefinitions()).thenReturn(variables);
  }

  @Test
  void should_collect_metrics() {

    MetricsCollectingExecutionListener listener = new MetricsCollectingExecutionListener();

    ExecutionContext global = new TestExecutionContext();
    ExecutionContext local1 = new TestExecutionContext();
    ExecutionContext local2 = new TestExecutionContext();
    ExecutionContext local3 = new TestExecutionContext();
    ExecutionContext local4 = new TestExecutionContext();

    listener.onExecutionStarted(successfulRead, global);
    listener.onExecutionStarted(failedRead, global);
    listener.onExecutionStarted(successfulWrite, global);
    listener.onExecutionStarted(failedWrite, global);

    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(0);
    assertThat(listener.getBytesSentMeter().get().getCount()).isEqualTo(0);
    assertThat(listener.getBytesReceivedMeter().get().getCount()).isEqualTo(0);

    listener.onReadRequestStarted(successfulRead, local1);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(1);
    listener.onReadRequestStarted(failedRead, local2);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(2);
    listener.onWriteRequestStarted(successfulWrite, local3);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(3);
    assertThat(listener.getBytesSentMeter().get().getCount()).isEqualTo(8);
    listener.onWriteRequestStarted(failedWrite, local4);
    assertThat(listener.getBytesSentMeter().get().getCount()).isEqualTo(16);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(4);

    listener.onReadRequestSuccessful(successfulRead, local1);
    // simulate 3 rows received
    listener.onRowReceived(row, local1);
    assertThat(listener.getBytesReceivedMeter().get().getCount()).isEqualTo(4);
    listener.onRowReceived(row, local1);
    assertThat(listener.getBytesReceivedMeter().get().getCount()).isEqualTo(8);
    listener.onRowReceived(row, local1);
    assertThat(listener.getBytesReceivedMeter().get().getCount()).isEqualTo(12);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(3);
    listener.onReadRequestFailed(failedRead, new RuntimeException(), local2);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(2);
    listener.onWriteRequestSuccessful(successfulWrite, local3);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(1);
    listener.onWriteRequestFailed(failedWrite, new RuntimeException(), local4);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(0);

    listener.onExecutionSuccessful(successfulRead, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedRead), global);
    listener.onExecutionSuccessful(successfulWrite, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedWrite), global);

    // 3 successful reads
    // 1 failed read
    // 2 successful writes
    // 2 failed writes

    assertThat(listener.getTotalStatementsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedStatementsCounter().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulStatementsCounter().getCount()).isEqualTo(2);

    assertThat(listener.getTotalReadsWritesTimer().getCount()).isEqualTo(8);
    assertThat(listener.getFailedReadsWritesCounter().getCount()).isEqualTo(3);
    assertThat(listener.getSuccessfulReadsWritesCounter().getCount()).isEqualTo(5);

    assertThat(listener.getTotalWritesTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedWritesCounter().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulWritesCounter().getCount()).isEqualTo(2);

    assertThat(listener.getTotalReadsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedReadsCounter().getCount()).isEqualTo(1);
    assertThat(listener.getSuccessfulReadsCounter().getCount()).isEqualTo(3);
  }

  @Test
  void should_not_collect_throughput_metrics() {

    MetricsCollectingExecutionListener listener =
        new MetricsCollectingExecutionListener(
            new MetricRegistry(), ProtocolVersion.DEFAULT, CodecRegistry.DEFAULT, false);

    assertThat(listener.getBytesSentMeter()).isNotPresent();
    assertThat(listener.getBytesReceivedMeter()).isNotPresent();

    // 3 successful reads
    // 1 failed read
    // 2 successful writes
    // 2 failed writes

    ExecutionContext global = new TestExecutionContext();
    ExecutionContext local1 = new TestExecutionContext();
    ExecutionContext local2 = new TestExecutionContext();
    ExecutionContext local3 = new TestExecutionContext();
    ExecutionContext local4 = new TestExecutionContext();

    listener.onExecutionStarted(successfulRead, global);
    listener.onExecutionStarted(failedRead, global);
    listener.onExecutionStarted(successfulWrite, global);
    listener.onExecutionStarted(failedWrite, global);

    listener.onReadRequestStarted(successfulRead, local1);
    listener.onReadRequestStarted(failedRead, local2);
    listener.onWriteRequestStarted(successfulWrite, local3);
    listener.onWriteRequestStarted(failedWrite, local4);

    listener.onReadRequestSuccessful(successfulRead, local1);
    listener.onRowReceived(row, local1);
    listener.onRowReceived(row, local1);
    listener.onRowReceived(row, local1);

    listener.onReadRequestFailed(failedRead, new RuntimeException(), local2);

    listener.onWriteRequestSuccessful(successfulWrite, local3);
    listener.onWriteRequestFailed(failedWrite, new RuntimeException(), local4);

    listener.onExecutionSuccessful(successfulRead, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedRead), global);
    listener.onExecutionSuccessful(successfulWrite, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedWrite), global);

    assertThat(listener.getBytesSentMeter()).isNotPresent();
    assertThat(listener.getBytesReceivedMeter()).isNotPresent();
  }

  private static class TestExecutionContext extends DefaultExecutionContext {
    @Override
    public long elapsedTimeNanos() {
      return 42;
    }
  }
}
