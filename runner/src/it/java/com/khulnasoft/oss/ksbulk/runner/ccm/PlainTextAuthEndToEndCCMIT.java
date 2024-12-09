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
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.createIpByCountryTable;
import static com.khulnasoft.oss.ksbulk.runner.tests.EndToEndUtils.validateOutputFiles;
import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDERR;

import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.ksbulk.runner.KhulnaSoftBulkLoader;
import com.khulnasoft.oss.ksbulk.runner.ExitStatus;
import com.khulnasoft.oss.ksbulk.runner.tests.CsvUtils;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMConfig;
import com.khulnasoft.oss.ksbulk.tests.driver.annotations.SessionConfig;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.utils.FileUtils;
import com.khulnasoft.oss.ksbulk.tests.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@CCMConfig(
    config = "authenticator:PasswordAuthenticator",
    jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0")
@Tag("medium")
class PlainTextAuthEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;

  PlainTextAuthEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(credentials = {"cassandra", "cassandra"}) CqlSession session,
      @LogCapture(loggerName = "com.khulnasoft.oss.ksbulk") LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
  }

  @AfterEach
  void truncateTable() {
    session.execute("TRUNCATE ip_by_country");
  }

  @ParameterizedTest(name = "[{index}] inferAuthProvider = {0}")
  @ValueSource(strings = {"true", "false"})
  void full_load_unload(boolean inferAuthProvider) throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--khulnasoft-java-driver.advanced.auth-provider.class");
      args.add("PlainTextAuthProvider");
    }

    args.add("--khulnasoft-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--khulnasoft-java-driver.advanced.auth-provider.password");
    args.add("cassandra");

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    FileUtils.deleteDirectory(logDir);
    logs.clear();
    stderr.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    if (!inferAuthProvider) {
      args.add("--khulnasoft-java-driver.advanced.auth-provider.class");
      args.add("PlainTextAuthProvider");
    }
    args.add("--khulnasoft-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--khulnasoft-java-driver.advanced.auth-provider.password");
    args.add("cassandra");

    status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(24, unloadDir);
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
  }

  @ParameterizedTest(name = "[{index}] inferAuthProvider = {0}")
  @ValueSource(strings = {"true", "false"})
  void full_load_unload_legacy_settings(boolean inferAuthProvider) throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--driver.auth.provider");
      args.add("PlainTextAuthProvider");
    }
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    ExitStatus status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    assertThat(logs)
        .hasMessageContaining(
            "Setting ksbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--khulnasoft-java-driver.advanced.auth-provider.* instead");
    assertThat(stderr.getStreamAsString())
        .contains(
            "Setting ksbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--khulnasoft-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    FileUtils.deleteDirectory(logDir);
    logs.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    if (!inferAuthProvider) {
      args.add("--driver.auth.provider");
      args.add("PlainTextAuthProvider");
    }
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    status = new KhulnaSoftBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(24, unloadDir);
    assertThat(logs)
        .hasMessageContaining(
            "Setting ksbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--khulnasoft-java-driver.advanced.auth-provider.* instead");
    assertThat(stderr.getStreamAsString())
        .contains(
            "Setting ksbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--khulnasoft-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
  }
}
