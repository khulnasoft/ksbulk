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
package com.khulnasoft.oss.ksbulk.runner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.slf4j.event.Level.ERROR;

import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.khulnasoft.oss.ksbulk.runner.cli.CommandLineParser;
import com.khulnasoft.oss.ksbulk.runner.cli.ParseException;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogConfigurationResource;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamType;
import com.khulnasoft.oss.ksbulk.tests.utils.FileUtils;
import com.khulnasoft.oss.ksbulk.tests.utils.StringUtils;
import com.khulnasoft.oss.ksbulk.workflow.api.WorkflowProvider;
import com.khulnasoft.oss.ksbulk.workflow.api.utils.WorkflowUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.assertj.core.util.Lists;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@LogConfigurationResource("logback.xml")
class KhulnaSoftBulkLoaderTest {

  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final LogInterceptor logs;
  private Path tempFolder;

  KhulnaSoftBulkLoaderTest(
      @StreamCapture(StreamType.STDOUT) StreamInterceptor stdOut,
      @StreamCapture(StreamType.STDERR) StreamInterceptor stdErr,
      @LogCapture(level = ERROR, loggerName = "com.khulnasoft.oss.ksbulk") LogInterceptor logs) {
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    this.logs = logs;
  }

  @AfterEach
  void resetAnsi() {
    AnsiConsole.systemUninstall();
  }

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
  }

  @AfterEach
  void deleteTempFolder() {
    FileUtils.deleteDirectory(tempFolder);
    FileUtils.deleteDirectory(Paths.get("./logs"));
  }

  @BeforeEach
  @AfterEach
  void resetConfig() {
    ConfigFactory.invalidateCaches();
    System.clearProperty("config.file");
  }

  @Test
  void should_show_global_help_when_no_args() {
    // global help, no shortcuts, has both json and csv settings.
    new KhulnaSoftBulkLoader().run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_opt_arg() {
    // global help, no shortcuts, has both json and csv settings.
    new KhulnaSoftBulkLoader("--help").run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_subcommand() {
    // global help, no shortcuts, has both json and csv settings.
    new KhulnaSoftBulkLoader("help").run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_section_help_when_help_opt_arg() {
    new KhulnaSoftBulkLoader("--help", "ksbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand() {
    new KhulnaSoftBulkLoader("help", "ksbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_global_help_filtered_when_help_opt_arg() {
    // global help, with shortcuts, has only json common settings.
    new KhulnaSoftBulkLoader("--help", "-c", "json").run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_global_help_filtered_when_help_subcommand() {
    // global help, with shortcuts, has only json common settings.
    new KhulnaSoftBulkLoader("help", "-c", "json").run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_section_help_when_help_opt_arg_with_connector() {
    new KhulnaSoftBulkLoader("--help", "-c", "json", "ksbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand_with_connector() {
    new KhulnaSoftBulkLoader("help", "-c", "json", "ksbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_error_when_junk_subcommand() {
    new KhulnaSoftBulkLoader("junk").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("First argument must be subcommand")
        .contains(", or \"help\"");
  }

  @Test
  void should_show_help_without_error_when_junk_subcommand_and_help() {
    new KhulnaSoftBulkLoader("junk", "--help").run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_help_without_error_when_good_subcommand_and_help() {
    new KhulnaSoftBulkLoader("load", "--help").run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_error_for_help_bad_section() {
    new KhulnaSoftBulkLoader("help", "noexist").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("noexist is not a valid section. Available sections include")
        .contains("ksbulk.connector.csv")
        .contains("ksbulk.batch")
        .contains("driver");
  }

  @Test
  void should_show_section_help() {
    new KhulnaSoftBulkLoader("help", "ksbulk.batch").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertSectionHelp();
    assertThat(out)
        .contains("--batch.mode,")
        .contains("--ksbulk.batch.mode <string>")
        .doesNotContain("This section has the following subsections");
  }

  @Test
  void should_show_section_help_with_subsection_pointers() {
    new KhulnaSoftBulkLoader("help", "ksbulk.connector").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out)
        .contains("--connector.name,")
        .contains("--ksbulk.connector.name")
        .contains("This section has the following subsections")
        .contains("connector.csv")
        .contains("connector.json");
  }

  @Test
  void should_show_section_help_with_connector_shortcuts() {
    new KhulnaSoftBulkLoader("help", "ksbulk.connector.csv").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains("-url,");
    assertThat(out).contains("--connector.csv.url,");
    assertThat(out).contains("--ksbulk.connector.csv.url <string>");
  }

  @Test
  void should_show_section_help_for_driver() {
    new KhulnaSoftBulkLoader("help", "driver").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains("Any valid driver setting can be specified on the command line");
    assertThat(out).contains("-h,");
    assertThat(out).contains("--driver.basic.contact-points,");
    assertThat(out).contains("--khulnasoft-java-driver.basic.contact-points <list<string>>");
    assertThat(out).contains("See the Java Driver online documentation for more information");
  }

  @Test
  void should_respect_custom_config_file() throws Exception {
    {
      Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
      Files.write(f, "ksbulk.connector.name=junk".getBytes(UTF_8));
      new KhulnaSoftBulkLoader("load", "-f", f.toString()).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Cannot find connector 'junk'");
    }
    logs.clear();
    {
      Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
      Files.write(
          f,
          ("ksbulk.connector.csv.url=/path/to/my/file\n"
                  + "ksbulk.schema.query=INSERT\n"
                  + "ksbulk.driver.socket.readTimeout=wonky")
              .getBytes(UTF_8));
      new KhulnaSoftBulkLoader("load", "-f", f.toString()).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Invalid value for ksbulk.driver.socket.readTimeout");
    }
    // DAT-221: -f should expand user home
    logs.clear();
    {
      Path f = Files.createTempFile(Paths.get(System.getProperty("user.home")), "myapp", ".conf");
      f.toFile().deleteOnExit();
      Files.write(f, "ksbulk.connector.name=foo".getBytes(UTF_8));
      new KhulnaSoftBulkLoader("load", "-f", "~/" + f.getFileName().toString()).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .doesNotContain("InvalidPathException")
          .contains("Cannot find connector 'foo'");
    }
  }

  @Test
  void should_error_out_for_bad_config_file() {
    new KhulnaSoftBulkLoader("load", "-f", "noexist").run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("Operation failed: Application file")
        .contains("noexist does not exist");
  }

  @Test
  void should_accept_connector_name_in_args_over_config_file() throws Exception {
    Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
    Files.write(f, "ksbulk.connector.name=junk".getBytes(UTF_8));
    new KhulnaSoftBulkLoader("load", "-c", "fromargs", "-f", f.toString()).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_error_on_populated_target_url_csv() throws Exception {
    Path unloadDir = null;
    try {
      unloadDir = createTempDirectory("test");
      Files.createFile(unloadDir.resolve("output-000001.csv"));
      new KhulnaSoftBulkLoader("unload", "--connector.csv.url", StringUtils.quoteJson(unloadDir))
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.csv.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        FileUtils.deleteDirectory(unloadDir);
      }
    }
  }

  @Test
  void should_error_on_populated_target_url_json() throws Exception {
    Path unloadDir = null;
    try {
      unloadDir = createTempDirectory("test");
      Files.createFile(unloadDir.resolve("output-000001.json"));
      new KhulnaSoftBulkLoader(
              "unload", "-c", "json", "--connector.json.url", StringUtils.quoteJson(unloadDir))
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.json.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        FileUtils.deleteDirectory(unloadDir);
      }
    }
  }

  @Test
  void should_handle_connector_name_long_option() {
    new KhulnaSoftBulkLoader("load", "--connector.name", "fromargs").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new KhulnaSoftBulkLoader("load", "--connector.name", "\"fromargs\"").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new KhulnaSoftBulkLoader("load", "--connector.name", "'fromargs'").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_handle_connector_name_long_option_with_equal() {
    new KhulnaSoftBulkLoader("load", "--connector.name=fromargs").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new KhulnaSoftBulkLoader("load", "--\"connector.name\"=\"fromargs\"").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new KhulnaSoftBulkLoader("load", "--'connector.name'='fromargs'").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_error_out_for_bad_execution_id_template() {
    new KhulnaSoftBulkLoader("load", "--engine.executionId", "%4$s").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Operation failed")
        .contains("Could not generate execution ID with template: '%4$s'");
  }

  @Test
  void should_accept_escaped_control_char() throws Exception {
    // control chars should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.delimiter", "\\t").parse().getConfig();
    assertThat(result.getString("ksbulk.connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_accept_escaped_backslash() throws Exception {
    // backslashes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.url", "C:\\\\Users").parse().getConfig();
    assertThat(result.getString("ksbulk.connector.csv.url")).isEqualTo("C:\\Users");
  }

  @Test
  void should_accept_escaped_double_quote() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.escape", "\\\"").parse().getConfig();
    assertThat(result.getString("ksbulk.connector.csv.escape")).isEqualTo("\"");
  }

  @Test
  void should_propagate_references() throws Exception {
    Config result =
        new CommandLineParser(
                "load",
                "--driver.basic.request.timeout",
                "10 minutes",
                "--driver.basic.request.page-size",
                "1234")
            .parse()
            .getConfig();
    assertThat(result.getString("khulnasoft-java-driver.basic.request.timeout"))
        .isEqualTo("10 minutes");
    assertThat(result.getString("khulnasoft-java-driver.advanced.metadata.schema.request-timeout"))
        .isEqualTo("10 minutes");
    assertThat(result.getString("khulnasoft-java-driver.basic.request.page-size")).isEqualTo("1234");
    assertThat(result.getString("khulnasoft-java-driver.advanced.metadata.schema.request-page-size"))
        .isEqualTo("1234");
  }

  @Test
  void should_accept_escaped_double_quote_in_complex_type() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--codec.booleanStrings", "[\"foo\\\"bar\"]")
            .parse()
            .getConfig();
    assertThat(result.getStringList("ksbulk.codec.booleanStrings")).containsExactly("foo\"bar");
  }

  @Test
  void should_not_add_quote_if_already_quoted() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.delimiter", "\"\\t\"").parse().getConfig();
    assertThat(result.getString("ksbulk.connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_not_accept_parse_error() {
    ParseException error =
        catchThrowableOfType(
            () -> new CommandLineParser("load", "--codec.booleanStrings", "[a,b").parse(),
            ParseException.class);
    assertThat(error)
        .hasMessageContaining(
            "Invalid value for ksbulk.codec.booleanStrings, expecting LIST, got: '[a,b'")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(error.getCause()).hasMessageContaining("h");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_short_options() {
    return Stream.of(
        Arguments.of("-locale", "locale", "ksbulk.codec.locale", "locale"),
        Arguments.of("-timeZone", "tz", "ksbulk.codec.timeZone", "tz"),
        Arguments.of("-c", "csv", "ksbulk.connector.name", "csv"),
        Arguments.of("-p", "pass", "khulnasoft-java-driver.advanced.auth-provider.password", "pass"),
        Arguments.of("-u", "user", "khulnasoft-java-driver.advanced.auth-provider.username", "user"),
        Arguments.of(
            "-h",
            "host1, host2",
            "khulnasoft-java-driver.basic.contact-points",
            Lists.newArrayList("host1", "host2")),
        Arguments.of(
            "-maxRetries", "42", "khulnasoft-java-driver.advanced.retry-policy.max-retries", 42),
        Arguments.of("-port", "9876", "khulnasoft-java-driver.basic.default-port", 9876),
        Arguments.of("-cl", "cl", "khulnasoft-java-driver.basic.request.consistency", "cl"),
        Arguments.of(
            "-dc",
            "dc1",
            "khulnasoft-java-driver.basic.load-balancing-policy.local-datacenter",
            "dc1"),
        Arguments.of(
            "-b",
            "/path/to/bundle",
            "khulnasoft-java-driver.basic.cloud.secure-connect-bundle",
            "/path/to/bundle"),
        Arguments.of("-maxErrors", "123", "ksbulk.log.maxErrors", 123),
        Arguments.of("-logDir", "logdir", "ksbulk.log.directory", "logdir"),
        Arguments.of("-jmx", "false", "ksbulk.monitoring.jmx", false),
        Arguments.of("-reportRate", "10 seconds", "ksbulk.monitoring.reportRate", "10 seconds"),
        Arguments.of("-k", "ks", "ksbulk.schema.keyspace", "ks"),
        Arguments.of(
            "-m",
            "{0:\"f1\", 1:\"f2\"}",
            "ksbulk.schema.mapping",
            "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of(
            "-nullStrings",
            "[nil, nada]",
            "ksbulk.codec.nullStrings",
            Lists.newArrayList("nil", "nada")),
        Arguments.of("-query", "INSERT INTO foo", "ksbulk.schema.query", "INSERT INTO foo"),
        Arguments.of("-t", "table", "ksbulk.schema.table", "table"),
        Arguments.of("-dryRun", "true", "ksbulk.engine.dryRun", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", shortOptionName, shortOptionValue).parse().getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_short_options() {
    return Stream.of(
        Arguments.of("-comment", "comment", "ksbulk.connector.csv.comment", "comment"),
        Arguments.of("-delim", "|", "ksbulk.connector.csv.delimiter", "|"),
        Arguments.of("-encoding", "enc", "ksbulk.connector.csv.encoding", "enc"),
        Arguments.of("-header", "header", "ksbulk.connector.csv.header", "header"),
        Arguments.of("-escape", "^", "ksbulk.connector.csv.escape", "^"),
        Arguments.of("-skipRecords", "3", "ksbulk.connector.csv.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "ksbulk.connector.csv.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "ksbulk.connector.csv.maxConcurrentFiles", "2C"),
        Arguments.of("-quote", "'", "ksbulk.connector.csv.quote", "'"),
        Arguments.of("-url", "http://findit", "ksbulk.connector.csv.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", shortOptionName, shortOptionValue).parse().getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_short_options() {
    return Stream.of(
        Arguments.of("-encoding", "enc", "ksbulk.connector.json.encoding", "enc"),
        Arguments.of("-skipRecords", "3", "ksbulk.connector.json.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "ksbulk.connector.json.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "ksbulk.connector.json.maxConcurrentFiles", "2C"),
        Arguments.of("-url", "http://findit", "ksbulk.connector.json.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "-c", "json", shortOptionName, shortOptionValue)
            .parse()
            .getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @Test
  void should_reject_concatenated_option_value() {
    assertThrows(
        ParseException.class,
        () -> new CommandLineParser("load", "-kks").parse(),
        "Unrecognized option: -kks");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_long_options() {
    return Stream.of(
        Arguments.of("ksbulk.driver.hosts", "host1, host2", Lists.newArrayList("host1", "host2")),
        Arguments.of("ksbulk.driver.port", "1", 1),
        Arguments.of("ksbulk.driver.protocol.compression", "NONE", "NONE"),
        Arguments.of("ksbulk.driver.pooling.local.connections", "2", 2),
        Arguments.of("ksbulk.driver.pooling.remote.connections", "3", 3),
        Arguments.of("ksbulk.driver.pooling.local.requests", "4", 4),
        Arguments.of("ksbulk.driver.pooling.remote.requests", "5", 5),
        Arguments.of("ksbulk.driver.pooling.heartbeat", "6 seconds", "6 seconds"),
        Arguments.of("ksbulk.driver.query.consistency", "cl", "cl"),
        Arguments.of("ksbulk.driver.query.serialConsistency", "serial-cl", "serial-cl"),
        Arguments.of("ksbulk.driver.query.fetchSize", "7", 7),
        Arguments.of("ksbulk.driver.query.idempotence", "false", false),
        Arguments.of("ksbulk.driver.socket.readTimeout", "8 seconds", "8 seconds"),
        Arguments.of("ksbulk.driver.auth.provider", "myauth", "myauth"),
        Arguments.of("ksbulk.driver.auth.username", "user", "user"),
        Arguments.of("ksbulk.driver.auth.password", "pass", "pass"),
        Arguments.of("ksbulk.driver.auth.authorizationId", "authid", "authid"),
        Arguments.of("ksbulk.driver.auth.principal", "user@foo.com", "user@foo.com"),
        Arguments.of("ksbulk.driver.auth.keyTab", "mykeytab", "mykeytab"),
        Arguments.of("ksbulk.driver.auth.saslService", "sasl", "sasl"),
        Arguments.of("ksbulk.driver.ssl.provider", "myssl", "myssl"),
        Arguments.of("ksbulk.driver.ssl.cipherSuites", "[TLS]", Lists.newArrayList("TLS")),
        Arguments.of("ksbulk.driver.ssl.truststore.path", "trust-path", "trust-path"),
        Arguments.of("ksbulk.driver.ssl.truststore.password", "trust-pass", "trust-pass"),
        Arguments.of("ksbulk.driver.ssl.truststore.algorithm", "trust-alg", "trust-alg"),
        Arguments.of("ksbulk.driver.ssl.keystore.path", "keystore-path", "keystore-path"),
        Arguments.of("ksbulk.driver.ssl.keystore.password", "keystore-pass", "keystore-pass"),
        Arguments.of("ksbulk.driver.ssl.keystore.algorithm", "keystore-alg", "keystore-alg"),
        Arguments.of("ksbulk.driver.ssl.openssl.keyCertChain", "key-cert-chain", "key-cert-chain"),
        Arguments.of("ksbulk.driver.ssl.openssl.privateKey", "key", "key"),
        Arguments.of("ksbulk.driver.timestampGenerator", "ts-gen", "ts-gen"),
        Arguments.of("ksbulk.driver.addressTranslator", "address-translator", "address-translator"),
        Arguments.of("ksbulk.driver.policy.lbp.dcAwareRoundRobin.localDc", "localDc", "localDc"),
        Arguments.of(
            "ksbulk.driver.policy.lbp.whiteList.hosts",
            "wh1, wh2",
            Lists.newArrayList("wh1", "wh2")),
        Arguments.of("ksbulk.driver.policy.maxRetries", "29", 29),
        Arguments.of("ksbulk.engine.dryRun", "true", true),
        Arguments.of("ksbulk.engine.executionId", "MY_EXEC_ID", "MY_EXEC_ID"),
        Arguments.of("ksbulk.batch.mode", "batch-mode", "batch-mode"),
        Arguments.of("ksbulk.batch.bufferSize", "9", 9),
        Arguments.of("ksbulk.batch.maxBatchSize", "10", 10),
        Arguments.of("ksbulk.executor.maxInFlight", "12", 12),
        Arguments.of("ksbulk.executor.maxPerSecond", "13", 13),
        Arguments.of("ksbulk.executor.continuousPaging.pageUnit", "BYTES", "BYTES"),
        Arguments.of("ksbulk.executor.continuousPaging.pageSize", "14", 14),
        Arguments.of("ksbulk.executor.continuousPaging.maxPages", "15", 15),
        Arguments.of("ksbulk.executor.continuousPaging.maxPagesPerSecond", "16", 16),
        Arguments.of("ksbulk.log.directory", "log-out", "log-out"),
        Arguments.of("ksbulk.log.maxErrors", "18", 18),
        Arguments.of("ksbulk.log.stmt.level", "NORMAL", "NORMAL"),
        Arguments.of("ksbulk.log.stmt.maxQueryStringLength", "19", 19),
        Arguments.of("ksbulk.log.stmt.maxBoundValues", "20", 20),
        Arguments.of("ksbulk.log.stmt.maxBoundValueLength", "21", 21),
        Arguments.of("ksbulk.log.stmt.maxInnerStatements", "22", 22),
        Arguments.of("ksbulk.codec.locale", "locale", "locale"),
        Arguments.of("ksbulk.codec.timeZone", "tz", "tz"),
        Arguments.of(
            "ksbulk.codec.booleanStrings", "[\"Si\", \"No\"]", Lists.newArrayList("Si", "No")),
        Arguments.of("ksbulk.codec.number", "codec-number", "codec-number"),
        Arguments.of("ksbulk.codec.timestamp", "codec-ts", "codec-ts"),
        Arguments.of("ksbulk.codec.date", "codec-date", "codec-date"),
        Arguments.of("ksbulk.codec.time", "codec-time", "codec-time"),
        Arguments.of("ksbulk.monitoring.reportRate", "23 sec", "23 sec"),
        Arguments.of("ksbulk.monitoring.rateUnit", "rate-unit", "rate-unit"),
        Arguments.of("ksbulk.monitoring.durationUnit", "duration-unit", "duration-unit"),
        Arguments.of("ksbulk.monitoring.expectedWrites", "24", 24),
        Arguments.of("ksbulk.monitoring.expectedReads", "25", 25),
        Arguments.of("ksbulk.monitoring.jmx", "false", false),
        Arguments.of("ksbulk.schema.keyspace", "ks", "ks"),
        Arguments.of("ksbulk.schema.table", "table", "table"),
        Arguments.of("ksbulk.schema.query", "SELECT JUNK", "SELECT JUNK"),
        Arguments.of(
            "ksbulk.schema.queryTimestamp", "2018-05-18T15:00:00Z", "2018-05-18T15:00:00Z"),
        Arguments.of("ksbulk.schema.queryTtl", "28", 28),
        Arguments.of("ksbulk.codec.nullStrings", "[NIL, NADA]", Lists.newArrayList("NIL", "NADA")),
        Arguments.of("ksbulk.schema.nullToUnset", "false", false),
        Arguments.of(
            "ksbulk.schema.mapping",
            "{0:\"f1\", 1:\"f2\"}",
            "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of("ksbulk.connector.name", "conn", "conn"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_long_options() {
    return Stream.of(
        Arguments.of("ksbulk.connector.csv.url", "url", "url"),
        Arguments.of("ksbulk.connector.csv.fileNamePattern", "pat", "pat"),
        Arguments.of("ksbulk.connector.csv.fileNameFormat", "fmt", "fmt"),
        Arguments.of("ksbulk.connector.csv.recursive", "true", true),
        Arguments.of("ksbulk.connector.csv.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("ksbulk.connector.csv.encoding", "enc", "enc"),
        Arguments.of("ksbulk.connector.csv.header", "false", false),
        Arguments.of("ksbulk.connector.csv.delimiter", "|", "|"),
        Arguments.of("ksbulk.connector.csv.quote", "'", "'"),
        Arguments.of("ksbulk.connector.csv.escape", "*", "*"),
        Arguments.of("ksbulk.connector.csv.comment", "#", "#"),
        Arguments.of("ksbulk.connector.csv.skipRecords", "2", 2),
        Arguments.of("ksbulk.connector.csv.maxRecords", "3", 3));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_long_options() {
    return Stream.of(
        Arguments.of("ksbulk.connector.json.url", "url", "url"),
        Arguments.of("ksbulk.connector.json.fileNamePattern", "pat", "pat"),
        Arguments.of("ksbulk.connector.json.fileNameFormat", "fmt", "fmt"),
        Arguments.of("ksbulk.connector.json.recursive", "true", true),
        Arguments.of("ksbulk.connector.json.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("ksbulk.connector.json.encoding", "enc", "enc"),
        Arguments.of("ksbulk.connector.json.skipRecords", "2", 2),
        Arguments.of("ksbulk.connector.json.maxRecords", "3", 3),
        Arguments.of("ksbulk.connector.json.mode", "SINGLE_DOCUMENT", "SINGLE_DOCUMENT"),
        Arguments.of(
            "ksbulk.connector.json.parserFeatures",
            "{f1 = true, f2 = false}",
            ImmutableMap.of("f1", true, "f2", false)),
        Arguments.of(
            "ksbulk.connector.json.generatorFeatures",
            "{g1 = true, g2 = false}",
            ImmutableMap.of("g1", true, "g2", false)),
        Arguments.of(
            "ksbulk.connector.json.serializationFeatures",
            "{s1 = true, s2 = false}",
            ImmutableMap.of("s1", true, "s2", false)),
        Arguments.of(
            "ksbulk.connector.json.deserializationFeatures",
            "{d1 = true, d2 = false}",
            ImmutableMap.of("d1", true, "d2", false)),
        Arguments.of("ksbulk.connector.json.prettyPrint", "true", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @Test
  void should_show_version_message_when_asked_long_option() {
    new KhulnaSoftBulkLoader("--version").run();
    String out = stdOut.getStreamAsString();
    assertThat(out).isEqualTo(String.format("%s%n", WorkflowUtils.getBulkLoaderNameAndVersion()));
  }

  @Test
  void should_show_version_message_when_asked_short_option() {
    new KhulnaSoftBulkLoader("-v").run();
    String out = stdOut.getStreamAsString();
    assertThat(out).isEqualTo(String.format("%s%n", WorkflowUtils.getBulkLoaderNameAndVersion()));
  }

  @Test
  void should_show_error_when_unload_and_dryRun() {
    new KhulnaSoftBulkLoader("unload", "-dryRun", "true", "-url", "/foo/bar", "-k", "k1", "-t", "t1")
        .run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Dry-run is not supported for unload");
  }

  @Test
  void should_error_on_backslash() throws URISyntaxException {
    Path badJson = Paths.get(ClassLoader.getSystemResource("bad-json.conf").toURI());
    new KhulnaSoftBulkLoader(
            "load",
            "-dryRun",
            "true",
            "-url",
            "/foo/bar",
            "-k",
            "k1",
            "-t",
            "t1",
            "-f",
            badJson.toString())
        .run();
    assertThat(stdErr.getStreamAsString())
        .contains(
            String.format(
                "Error parsing configuration file %s at line 1. "
                    + "Please make sure its format is compliant with HOCON syntax. "
                    + "If you are using \\ (backslash) to define a path, "
                    + "escape it with \\\\ or use / (forward slash) instead.",
                badJson));
  }

  private void assertGlobalHelp(boolean jsonOnly) {
    String out =
        new AnsiString(stdOut.getStreamAsString()).getPlain().toString().replaceAll("[\\s]+", " ");

    assertThat(out).contains(WorkflowUtils.getBulkLoaderNameAndVersion());
    assertThat(out).contains("-v, --version Show program's version number and exit");

    ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class);
    for (WorkflowProvider workflowProvider : loader) {
      assertThat(out).contains(workflowProvider.getTitle());
      assertThat(out).contains(workflowProvider.getDescription());
    }

    // The following assures us that we're looking at global help, not section help.
    assertThat(out).contains("GETTING MORE HELP");

    // The tests try restricting global help to json connector, or show all connectors.
    // If all, shortcut options for connector settings should not be shown.
    // If restricted to json, show the shortcut options for common json settings.
    assertThat(out).contains("--connector.json.url");
    assertThat(out).contains("--ksbulk.connector.json.url");
    if (jsonOnly) {
      assertThat(out).contains("-url,");
      assertThat(out).contains("--connector.json.url,");
      assertThat(out).contains("--ksbulk.connector.json.url <string>");
      assertThat(out).doesNotContain("connector.csv.url");
    } else {
      assertThat(out).contains("--connector.csv.url,");
      assertThat(out).contains("--ksbulk.connector.csv.url <string>");
      assertThat(out).doesNotContain("-url");
    }
    assertThat(out).doesNotContain("First argument must be subcommand");
    assertThat(out).containsPattern("-f <string>\\s+Load options from the given file");
  }

  private void assertSectionHelp() {
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains(WorkflowUtils.getBulkLoaderNameAndVersion());

    // The following assures us that we're looking at section help, not global help.
    assertThat(out).doesNotContain("GETTING MORE HELP");
    assertThat(out).doesNotContain("--connector.json.url");
    assertThat(out).doesNotContain("--ksbulk.connector.json.url");
    assertThat(out).doesNotContain("--connector.csv.url");
    assertThat(out).doesNotContain("--ksbulk.connector.csv.url");

    assertThat(out).contains("--batch.mode,");
    assertThat(out).contains("--ksbulk.batch.mode <string>");
  }
}
