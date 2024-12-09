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

import static com.khulnasoft.oss.ksbulk.tests.assertions.TestAssertions.assertThat;
import static com.khulnasoft.oss.ksbulk.tests.utils.ReflectionUtils.getInternalState;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.khulnasoft.dse.driver.api.core.DseProtocolVersion;
import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.driver.api.core.config.DefaultDriverOption;
import com.khulnasoft.oss.driver.api.core.config.DriverExecutionProfile;
import com.khulnasoft.oss.driver.api.core.metadata.Node;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.khulnasoft.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.khulnasoft.oss.ksbulk.executor.api.reader.ReactiveBulkReader;
import com.khulnasoft.oss.ksbulk.executor.api.writer.ReactiveBulkWriter;
import com.khulnasoft.oss.ksbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.khulnasoft.oss.ksbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.khulnasoft.oss.ksbulk.tests.driver.DriverUtils;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class ExecutorSettingsTest {

  private CqlSession session;

  @BeforeEach
  void setUp() {
    session = DriverUtils.mockSession();
  }

  @Test
  void should_create_non_continuous_executor_when_write_workflow() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkWriter executor = settings.newWriteExecutor(session, null);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_session_not_dse(
      @LogCapture LogInterceptor logs) {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .doesNotHaveMessageContaining(
            "Continuous paging is not available, read performance will not be optimal");
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_session_dse_but_wrong_CL(
      @LogCapture LogInterceptor logs) {
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("TWO");
    when(session.getContext().getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V1);
    mockNode();
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is not available, read performance will not be optimal");
  }

  @Test
  void should_create_continuous_executor_when_read_workflow_and_session_dse() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor");
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("LOCAL_ONE");
    when(session.getContext().getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V1);
    mockNode();
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(ContinuousReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_not_enabled() {
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    when(session.getContext().getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V1);
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "continuousPaging.enabled", false);
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_search_query(
      @LogCapture LogInterceptor logs) {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "continuousPaging.enabled", true);
    when(session.getContext().getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V1);
    mockNode();
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, true);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is enabled but is not compatible with search queries; disabling");
  }

  @Test
  void should_enable_maxPerSecond() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor", "maxPerSecond", 100);
    ExecutorSettings settings = new ExecutorSettings(config);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(((RateLimiter) getInternalState(executor, "rateLimiter")).getRate()).isEqualTo(100);
  }

  @Test
  void should_disable_maxPerSecond() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor", "maxPerSecond", 0);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(getInternalState(executor, "rateLimiter")).isNull();
  }

  @Test
  void should_throw_exception_when_maxPerSecond_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "maxPerSecond", "NotANumber");
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.executor.maxPerSecond, expecting NUMBER, got STRING");
  }

  @Test
  void should_enable_maxBytesPerSecond() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "maxBytesPerSecond", "1 kilobyte");
    ExecutorSettings settings = new ExecutorSettings(config);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(((RateLimiter) getInternalState(executor, "bytesRateLimiter")).getRate())
        .isEqualTo(1000);
  }

  @Test
  void should_disable_maxBytesPerSecond() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor", "maxBytesPerSecond", -1);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(getInternalState(executor, "bytesRateLimiter")).isNull();
  }

  @Test
  void should_throw_exception_when_maxBytesPerSecond_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "maxBytesPerSecond", "NotANumber");
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.executor.maxBytesPerSecond, expecting NUMBER or STRING in size-in-bytes format, got 'NotANumber'");
  }

  @Test
  void should_enable_maxInFlight() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor", "maxInFlight", 100);
    ExecutorSettings settings = new ExecutorSettings(config);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    Semaphore maxConcurrentRequests =
        (Semaphore) getInternalState(executor, "maxConcurrentRequests");
    assertThat(maxConcurrentRequests.availablePermits()).isEqualTo(100);
  }

  @Test
  void should_disable_maxInFlight() {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor", "maxInFlight", 0);
    ExecutorSettings settings = new ExecutorSettings(config);
    DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    Semaphore maxConcurrentRequests =
        (Semaphore) getInternalState(executor, "maxConcurrentRequests");
    assertThat(maxConcurrentRequests).isNull();
  }

  @Test
  void should_throw_exception_when_maxInFlight_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("ksbulk.executor", "maxInFlight", "NotANumber");
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for ksbulk.executor.maxInFlight, expecting NUMBER, got STRING");
  }

  @Test
  void should_log_warning_when_concurrentMaxQueries_is_user_defined(
      @LogCapture LogInterceptor logs) {
    Config config =
        TestConfigUtils.createTestConfig(
            "ksbulk.executor", "continuousPaging.maxConcurrentQueries", "10");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    assertThat(logs)
        .hasMessageContaining(
            "Setting executor.continuousPaging.maxConcurrentQueries has been removed and is not honored anymore");
  }

  @Test
  void should_log_warning_when_cloud_and_maxPerSecond_not_defined(@LogCapture LogInterceptor logs) {
    Config config = TestConfigUtils.createTestConfig("ksbulk.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    settings.enforceCloudRateLimit(3);
    assertThat(logs)
        .hasMessageContaining(
            "Setting executor.maxPerSecond not set when connecting to KhulnaSoft Astra: applying a limit of 9,000 ops/second");
  }

  private void mockNode() {
    Node node = DriverUtils.mockNode();
    Map<UUID, Node> nodes = ImmutableMap.of(Objects.requireNonNull(node.getHostId()), node);
    when(session.getMetadata().getNodes()).thenReturn(nodes);
  }
}
