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
package com.khulnasoft.oss.ksbulk.workflow.commons.settings;

import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDERR;
import static com.khulnasoft.oss.ksbulk.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.joran.spi.JoranException;
import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.ksbulk.tests.driver.DriverUtils;
import com.khulnasoft.oss.ksbulk.tests.logging.LogUtils;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.utils.FileUtils;
import com.khulnasoft.oss.ksbulk.tests.utils.TestConfigUtils;
import com.khulnasoft.oss.ksbulk.workflow.api.error.AbsoluteErrorThreshold;
import com.khulnasoft.oss.ksbulk.workflow.api.error.ErrorThreshold;
import com.khulnasoft.oss.ksbulk.workflow.api.error.RatioErrorThreshold;
import com.khulnasoft.oss.ksbulk.workflow.api.error.UnlimitedErrorThreshold;
import com.khulnasoft.oss.ksbulk.workflow.commons.log.LogManager;
import com.khulnasoft.oss.ksbulk.workflow.commons.settings.LogSettings.Verbosity;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(StreamInterceptingExtension.class)
class LogSettingsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettingsTest.class);

  private CqlSession session;
  private String executionId;
  private Path defaultLogsDirectory;
  private Path customLogsDirectory;

  @BeforeEach
  void setUp() {
    session = DriverUtils.mockSession();
    executionId = "test" + System.nanoTime();
  }

  @BeforeEach
  void createLogsDirectories() throws IOException {
    defaultLogsDirectory = Paths.get("./target/logs");
    customLogsDirectory = Files.createTempDirectory("logs");
    Files.createDirectories(defaultLogsDirectory);
    Files.createDirectories(customLogsDirectory);
  }

  @AfterEach
  void deleteLogsDirectories() {
    FileUtils.deleteDirectory(defaultLogsDirectory);
    FileUtils.deleteDirectory(customLogsDirectory);
  }

  @BeforeEach
  void resetLogbackConfiguration() throws JoranException {
    // Tests in this class require the production log configuration file
    LogUtils.resetLogbackConfiguration("logback.xml");
  }

  @Test
  void should_create_log_manager_with_default_output_directory() throws Exception {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log");
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    try (LogManager logManager = settings.newLogManager(session)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getOperationDirectory().toFile().getAbsolutePath())
          .isEqualTo(
              defaultLogsDirectory.resolve(executionId).normalize().toFile().getAbsolutePath());
    }
  }

  @Test()
  void should_accept_maxErrors_as_absolute_number() throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", 20);
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(AbsoluteErrorThreshold.class);
    assertThat(((AbsoluteErrorThreshold) threshold).getMaxErrors()).isEqualTo(20);
  }

  @Test()
  void should_accept_maxErrors_as_percentage() throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", "20%");
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(RatioErrorThreshold.class);
    assertThat(((RatioErrorThreshold) threshold).getMaxErrorRatio()).isEqualTo(0.2f);
    // min sample is fixed and cannot be changed by the user currently
    assertThat(((RatioErrorThreshold) threshold).getMinSample()).isEqualTo(100);
  }

  @Test()
  void should_disable_maxErrors() throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", -42);
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(UnlimitedErrorThreshold.class);
  }

  @Test()
  void should_error_when_percentage_is_out_of_bounds() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", "112 %");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", "0%");

    LogSettings settings2 = new LogSettings(config, executionId);
    assertThatThrownBy(settings2::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config = TestConfigUtils.createTestConfig("ksbulk.log", "maxErrors", "-1%");

    LogSettings settings3 = new LogSettings(config, executionId);
    assertThatThrownBy(settings3::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
  }

  @Test
  void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "directory", quoteJson(customLogsDirectory));
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    try (LogManager logManager = settings.newLogManager(session)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getOperationDirectory().toFile())
          .isEqualTo(customLogsDirectory.resolve(executionId).toFile());
    }
  }

  @Test
  void should_log_to_main_log_file_in_normal_mode(@StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "directory", quoteJson(customLogsDirectory));
    ch.qos.logback.classic.Logger ksbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.khulnasoft.oss.ksbulk");
    Level oldLevel = ksbulkLogger.getLevel();
    try {
      LogSettings settings = new LogSettings(config, executionId);
      settings.init();
      assertThat(ksbulkLogger.getLevel()).isEqualTo(Level.INFO);
      ksbulkLogger.info("this is a test 1");
      ksbulkLogger.debug("this should not appear");
      LOGGER.info("this is a test 2");
      LOGGER.debug("this should not appear");
      // driver log level should be WARN
      Logger ossDriverLogger = LoggerFactory.getLogger("com.khulnasoft.oss.driver");
      ossDriverLogger.warn("this is a test 3");
      ossDriverLogger.info("this should not appear");
      Logger dseDriverLogger = LoggerFactory.getLogger("com.khulnasoft.dse.driver");
      dseDriverLogger.warn("this is a test 4");
      dseDriverLogger.info("this should not appear");
      Path logFile = customLogsDirectory.resolve(executionId).resolve("operation.log");
      assertThat(logFile).exists();
      List<String> contents = Files.readAllLines(logFile);
      assertThat(contents)
          .anySatisfy(line -> assertThat(line).endsWith("this is a test 1"))
          .anySatisfy(line -> assertThat(line).endsWith("this is a test 2"))
          .anySatisfy(line -> assertThat(line).endsWith("this is a test 3"))
          .anySatisfy(line -> assertThat(line).endsWith("this is a test 4"))
          .noneSatisfy(line -> assertThat(line).contains("this should not appear"));
      assertThat(stderr.getStreamLinesPlain())
          .contains("this is a test 1", "this is a test 2", "this is a test 3", "this is a test 4")
          .doesNotContain("this should not appear");
    } finally {
      ksbulkLogger.setLevel(oldLevel);
    }
  }

  @Test
  void should_log_to_main_log_file_in_quiet_mode(@StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Config config =
        TestConfigUtils.createTestConfig(
            "ksbulk.log", "directory", quoteJson(customLogsDirectory), "verbosity", "quiet");
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ch.qos.logback.classic.Logger ksbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.khulnasoft.oss.ksbulk");
    ksbulkLogger.warn("this is a test 1");
    ksbulkLogger.info("this should not appear");
    LOGGER.warn("this is a test 2");
    LOGGER.info("this should not appear");
    Logger ossDriverLogger = LoggerFactory.getLogger("com.khulnasoft.oss.driver");
    ossDriverLogger.warn("this is a test 3");
    ossDriverLogger.info("this should not appear");
    Logger dseDriverLogger = LoggerFactory.getLogger("com.khulnasoft.dse.driver");
    dseDriverLogger.warn("this is a test 4");
    dseDriverLogger.info("this should not appear");
    Path logFile = customLogsDirectory.resolve(executionId).resolve("operation.log");
    assertThat(logFile).exists();
    List<String> contents = Files.readAllLines(logFile);
    assertThat(contents)
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 1"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 2"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 3"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 4"))
        .noneSatisfy(line -> assertThat(line).contains("this should not appear"));
    assertThat(stderr.getStreamLinesPlain())
        .contains("this is a test 1", "this is a test 2", "this is a test 3", "this is a test 4")
        .doesNotContain("this should not appear");
  }

  @Test
  void should_log_to_main_log_file_in_verbose_mode(@StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Config config =
        TestConfigUtils.createTestConfig(
            "ksbulk.log", "directory", quoteJson(customLogsDirectory), "verbosity", "high");
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ch.qos.logback.classic.Logger ksbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.khulnasoft.oss.ksbulk");
    assertThat(ksbulkLogger.getLevel()).isEqualTo(Level.DEBUG);
    ksbulkLogger.debug("this is a test 1");
    LOGGER.debug("this is a test 2");
    // driver log level should now be INFO
    LoggerFactory.getLogger("com.khulnasoft.oss.driver").info("this is a test 3");
    LoggerFactory.getLogger("com.khulnasoft.oss.driver").debug("this should not appear");
    LoggerFactory.getLogger("com.khulnasoft.dse.driver").info("this is a test 4");
    LoggerFactory.getLogger("com.khulnasoft.dse.driver").debug("this should not appear");
    Path logFile = customLogsDirectory.resolve(executionId).resolve("operation.log");
    assertThat(logFile).exists();
    List<String> contents = Files.readAllLines(logFile);
    assertThat(contents)
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 1"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 2"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 3"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 4"))
        .noneSatisfy(line -> assertThat(line).contains("this should not appear"));
    assertThat(stderr.getStreamLinesPlain())
        .contains("this is a test 1", "this is a test 2", "this is a test 3", "this is a test 4")
        .doesNotContain("this should not appear");
  }

  @Test
  void should_log_to_main_log_file_in_debug_mode(@StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Config config =
        TestConfigUtils.createTestConfig(
            "ksbulk.log", "directory", quoteJson(customLogsDirectory), "verbosity", "max");
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ch.qos.logback.classic.Logger ksbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.khulnasoft.oss.ksbulk");
    assertThat(ksbulkLogger.getLevel()).isEqualTo(Level.TRACE);
    ksbulkLogger.trace("this is a test 1");
    LOGGER.trace("this is a test 2");
    // driver log level should now be INFO
    LoggerFactory.getLogger("com.khulnasoft.oss.driver").trace("this is a test 3");
    LoggerFactory.getLogger("com.khulnasoft.dse.driver").trace("this is a test 4");
    Path logFile = customLogsDirectory.resolve(executionId).resolve("operation.log");
    assertThat(logFile).exists();
    List<String> contents = Files.readAllLines(logFile);
    assertThat(contents)
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 1"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 2"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 3"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 4"));
    assertThat(stderr.getStreamLinesPlain())
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 1"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 2"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 3"))
        .anySatisfy(line -> assertThat(line).endsWith("this is a test 4"));
  }

  @Test
  void should_throw_exception_when_maxQueryStringLength_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "stmt.maxQueryStringLength", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.stmt.maxQueryStringLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValueLength_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "stmt.maxBoundValueLength", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.stmt.maxBoundValueLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValues_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "stmt.maxBoundValues", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.stmt.maxBoundValues, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValues_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "row.maxResultSetValues", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.row.maxResultSetValues, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValueLength_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "row.maxResultSetValueLength", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.row.maxResultSetValueLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxInnerStatements_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "stmt.maxInnerStatements", "NotANumber");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.stmt.maxInnerStatements, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_level_invalid() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "stmt.level", "NotALevel");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.stmt.level, expecting one of ABRIDGED, NORMAL, EXTENDED, got: 'NotALevel'");
  }

  @Test()
  void should_accept_maxQueryWarnings_as_absolute_number() throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxQueryWarnings", 20);
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ErrorThreshold threshold = settings.queryWarningsThreshold;
    assertThat(threshold).isInstanceOf(AbsoluteErrorThreshold.class);
    assertThat(((AbsoluteErrorThreshold) threshold).getMaxErrors()).isEqualTo(20);
  }

  @Test()
  void should_not_accept_maxQueryWarnings_as_percentage() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxQueryWarnings", "20%");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.maxQueryWarnings, expecting NUMBER, got STRING");
  }

  @Test()
  void should_disable_maxQueryWarnings() throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "maxQueryWarnings", -42);
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    ErrorThreshold threshold = settings.queryWarningsThreshold;
    assertThat(threshold).isInstanceOf(UnlimitedErrorThreshold.class);
  }

  @Test()
  void should_log_warning_on_deprecated_verbosity_level(
      @StreamCapture(STDERR) StreamInterceptor stderr) throws IOException {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "verbosity", 2);
    LogSettings settings = new LogSettings(config, executionId);
    settings.init();
    assertThat(settings.getVerbosity()).isEqualTo(Verbosity.high);
    assertThat(stderr.getStreamLinesPlain())
        .contains(
            "Numeric verbosity levels are deprecated, use 'quiet' (0), 'normal' (1), 'high' (2) or 'max' (3) instead.");
  }

  @Test
  void should_throw_exception_when_numeric_verbosity_not_valid() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.log", "verbosity", -1);
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid numeric value for ksbulk.log.verbosity, expecting one of: 0 (quiet), 1 (normal), 2 (high) or 3 (max), got: -1");
  }

  @Test
  void should_throw_exception_when_string_verbosity_not_valid() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.log", "verbosity", "NotAValidVerbosity");
    LogSettings settings = new LogSettings(config, executionId);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.log.verbosity, expecting one of quiet, normal, high, max, got: 'NotAValidVerbosity'");
  }
}
