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
package com.khulnasoft.oss.ksbulk.runner.ccm;

import static com.khulnasoft.oss.ksbulk.runner.ExitStatus.STATUS_OK;
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDERR;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_MINUTE;
import static org.slf4j.event.Level.WARN;

import com.khulnasoft.oss.driver.api.core.CqlIdentifier;
import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.ksbulk.connectors.api.DefaultMappedField;
import com.khulnasoft.oss.ksbulk.connectors.api.Record;
import com.khulnasoft.oss.ksbulk.runner.KhulnaSoftBulkLoader;
import com.khulnasoft.oss.ksbulk.runner.ExitStatus;
import com.khulnasoft.oss.ksbulk.runner.tests.MockConnector;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster.Type;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster.Workload;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMConfig;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMRequirements;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMWorkload;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
@CCMConfig(workloads = @CCMWorkload(Workload.solr))
@CCMRequirements(
    compatibleTypes = Type.DSE,
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "5.1"))
class SearchEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;

  private List<Record> records;

  SearchEndToEndCCMIT(
      CCMCluster ccm,
      CqlSession session,
      @LogCapture(level = WARN, loggerName = "com.khulnasoft.oss.ksbulk") LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeEach
  void setUpConnector() {
    records = MockConnector.mockWrites();
  }

  /** Test for DAT-309: unload of a Solr query */
  @Test
  void full_unload_search_solr_query() {

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_search (pk int, cc int, v varchar, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE SEARCH INDEX IF NOT EXISTS ON test_search WITH COLUMNS v { indexed:true };");

    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 0, 'foo')");
    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 1, 'bar')");
    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 2, 'qix')");

    String query = "SELECT v FROM test_search WHERE solr_query = '{\"q\": \"v:foo\"}'";

    // Wait until index is built
    await().atMost(ONE_MINUTE).until(() -> !session.execute(query).all().isEmpty());

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--executor.continuousPaging.enabled");
    args.add("true");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.query");
    args.add(StringUtils.quoteJson(query));

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertThat(logs)
        .hasMessageContaining("completed successfully")
        .hasMessageContaining(
            "Continuous paging is enabled but is not compatible with search queries; disabling");
    assertThat(stderr.getStreamAsStringPlain())
        .contains("completed successfully")
        .contains(
            "Continuous paging is enabled but is not compatible with search queries; disabling");

    assertThat(records)
        .singleElement()
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(1);
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("foo");
            });
  }

  /**
   * Test for DAT-365: regular unload of a search-enabled table should not contain the solr_query
   * column.
   */
  @Test
  void normal_unload_of_search_enabled_table() {

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_search2 (pk int, cc int, v varchar, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE SEARCH INDEX IF NOT EXISTS ON test_search2 WITH COLUMNS v { indexed:true };");

    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 0, 'foo')");
    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 1, 'bar')");
    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 2, 'qix')");

    // Wait until index is built
    await()
        .atMost(ONE_MINUTE)
        .until(
            () ->
                !session
                    .execute("SELECT v FROM test_search2 WHERE solr_query = '{\"q\": \"v:foo\"}'")
                    .all()
                    .isEmpty());

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("test_search2");

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertThat(records)
        .hasSize(3)
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("foo");
            },
            Index.atIndex(0))
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("1");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("bar");
            },
            Index.atIndex(1))
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("2");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("qix");
            },
            Index.atIndex(2));
  }
}
