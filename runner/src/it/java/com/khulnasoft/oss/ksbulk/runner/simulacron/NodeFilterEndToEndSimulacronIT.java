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
package com.khulnasoft.oss.ksbulk.runner.simulacron;

import static com.khulnasoft.oss.driver.api.core.type.DataTypes.TEXT;
import static com.khulnasoft.oss.ksbulk.runner.ExitStatus.STATUS_ABORTED_FATAL_ERROR;
import static com.khulnasoft.oss.ksbulk.runner.ExitStatus.STATUS_OK;
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDERR;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDOUT;

import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.khulnasoft.oss.ksbulk.connectors.api.DefaultResource;
import com.khulnasoft.oss.ksbulk.connectors.api.Record;
import com.khulnasoft.oss.ksbulk.connectors.api.Resource;
import com.khulnasoft.oss.ksbulk.connectors.csv.CSVConnector;
import com.khulnasoft.oss.ksbulk.runner.KhulnaSoftBulkLoader;
import com.khulnasoft.oss.ksbulk.runner.ExitStatus;
import com.khulnasoft.oss.ksbulk.runner.tests.MockConnector;
import com.khulnasoft.oss.ksbulk.runner.tests.RecordUtils;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronUtils;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronUtils.Column;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronUtils.Table;
import com.khulnasoft.oss.ksbulk.tests.simulacron.annotations.SimulacronConfig;
import com.khulnasoft.oss.ksbulk.tests.utils.StringUtils;
import com.khulnasoft.oss.simulacron.server.BoundCluster;
import com.khulnasoft.oss.simulacron.server.BoundNode;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@SimulacronConfig(numberOfNodes = {3})
class NodeFilterEndToEndSimulacronIT extends EndToEndSimulacronITBase {

  private final LogInterceptor driverLogs;

  NodeFilterEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(loggerName = "com.khulnasoft.oss.ksbulk") LogInterceptor logs,
      @LogCapture(
              loggerName = "com.khulnasoft.oss.driver.internal.core.loadbalancing",
              level = Level.DEBUG)
          LogInterceptor driverLogs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    super(simulacron, logs, stdOut, stdErr);
    this.driverLogs = driverLogs;
  }

  @BeforeAll
  void mockConnector() {
    MockConnector.setDelegate(new MockCSVConnector());
  }

  @BeforeEach
  void primeTables() {
    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace(
            "ks1",
            new Table(
                "table1", new Column("pk", TEXT), new Column("cc", TEXT), new Column("v", TEXT))));
  }

  @ParameterizedTest
  @MethodSource("nodesList")
  void node_filter_allow(Set<Integer> allow) {

    String[] args = {
      "load",
      "-c",
      "mock",
      "--batch.mode",
      "DISABLED",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1",
      "-port",
      String.valueOf(port),
      "-allow",
      getHostList(allow)
    };

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs)
        .hasMessageContaining("completed successfully")
        .hasMessageContaining("Records: total: 1,000, successful: 1,000, failed: 0");
    for (int i = 0; i < 3; i++) {
      BoundNode node = simulacron.node(i);
      InetSocketAddress addr = node.inetSocketAddress();
      String hostName = Pattern.quote(addr.getAddress().getHostAddress());
      int port = addr.getPort();
      if (allow.isEmpty() || allow.contains(i)) {
        checkAllowed(node, hostName, port);
      } else {
        checkExcluded(node, hostName, port);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("nodesList")
  void node_filter_allow_deprecated(Set<Integer> allow) {

    String[] args = {
      "load",
      "-c",
      "mock",
      "--batch.mode",
      "DISABLED",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1",
      "-port",
      String.valueOf(port),
      "--ksbulk.driver.policy.lbp.whiteList.hosts",
      getHostList(allow)
    };

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs)
        .hasMessageContaining("Setting ksbulk.driver.policy.lbp.whiteList.hosts is deprecated")
        .hasMessageContaining(
            "configure the driver directly using --khulnasoft-java-driver.basic.load-balancing-policy.evaluator.allow (or -allow) instead.")
        .hasMessageContaining("completed successfully")
        .hasMessageContaining("Records: total: 1,000, successful: 1,000, failed: 0");
    for (int i = 0; i < 3; i++) {
      BoundNode node = simulacron.node(i);
      InetSocketAddress addr = node.inetSocketAddress();
      String hostName = Pattern.quote(addr.getAddress().getHostAddress());
      int port = addr.getPort();
      if (allow.isEmpty() || allow.contains(i)) {
        checkAllowed(node, hostName, port);
      } else {
        checkExcluded(node, hostName, port);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("nodesList")
  void node_filter_deny(Set<Integer> deny) {

    String[] args = {
      "load",
      "-c",
      "mock",
      "--batch.mode",
      "DISABLED",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1",
      "-port",
      String.valueOf(port),
      "-deny",
      getHostList(deny)
    };

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    if (deny.size() == 3) {
      assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
      assertThat(logs).hasMessageContaining("No node was available to execute the query");
    } else {
      assertStatus(status, STATUS_OK);
      assertThat(logs)
          .hasMessageContaining("completed successfully")
          .hasMessageContaining("Records: total: 1,000, successful: 1,000, failed: 0");
      for (int i = 0; i < 3; i++) {
        BoundNode node = simulacron.node(i);
        InetSocketAddress addr = node.inetSocketAddress();
        String hostName = Pattern.quote(addr.getAddress().getHostAddress());
        int port = addr.getPort();
        if (deny.contains(i)) {
          checkExcluded(node, hostName, port);
        } else {
          checkAllowed(node, hostName, port);
        }
      }
    }
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> nodesList() {
    return Stream.of(
        Arguments.of(ImmutableSet.of()),
        Arguments.of(ImmutableSet.of(0)),
        Arguments.of(ImmutableSet.of(1)),
        Arguments.of(ImmutableSet.of(2)),
        Arguments.of(ImmutableSet.of(0, 1)),
        Arguments.of(ImmutableSet.of(1, 2)),
        Arguments.of(ImmutableSet.of(0, 2)),
        Arguments.of(ImmutableSet.of(0, 1, 2)));
  }

  private String getHostList(Collection<Integer> nodeIndices) {
    return "["
        + nodeIndices.stream()
            .map(simulacron::node)
            .map(BoundNode::inetSocketAddress)
            .map(InetSocketAddress::getAddress)
            .map(InetAddress::getHostAddress)
            .map(StringUtils::quoteJson)
            .collect(Collectors.joining(","))
        + "]";
  }

  private long getWritesCount(BoundNode node) {
    return node.getLogs().getQueryLogs().stream()
        .filter(l -> l.getType().equals("EXECUTE"))
        .filter(l -> l.getQuery().contains("INSERT INTO"))
        .count();
  }

  private void checkAllowed(BoundNode node, String hostName, int port) {
    assertThat(driverLogs)
        .hasMessageMatching(
            "Evaluator did not assign a distance to node Node.+" + hostName + ":" + port);
    assertThat(getWritesCount(node)).isNotZero();
  }

  private void checkExcluded(BoundNode node, String hostName, int port) {
    assertThat(driverLogs)
        .hasMessageMatching(
            "Evaluator assigned distance IGNORED to node Node.+" + hostName + ":" + port);
    assertThat(getWritesCount(node)).isZero();
  }

  private static class MockCSVConnector extends CSVConnector {

    @Override
    public void configure(@NonNull Config settings, boolean read, boolean retainRecordSources) {}

    @Override
    public void init() {}

    @Override
    public int readConcurrency() {
      return 1;
    }

    @NonNull
    @Override
    public Publisher<Resource> read() {
      List<Record> records = new ArrayList<>(1000);
      URI resource = URI.create("file://file");
      for (int i = 0; i < 1000; i++) {
        Record record =
            RecordUtils.indexedCSV(
                resource,
                i + 1,
                "pk",
                String.valueOf(i),
                "cc",
                String.valueOf(i),
                "v",
                String.valueOf(i));
        records.add(record);
      }
      return Flux.just(new DefaultResource(resource, Flux.fromIterable(records)));
    }
  }
}
