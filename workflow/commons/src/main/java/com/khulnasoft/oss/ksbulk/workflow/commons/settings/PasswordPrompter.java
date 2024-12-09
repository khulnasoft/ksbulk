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

import com.khulnasoft.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.khulnasoft.oss.ksbulk.config.ConfigUtils;
import com.khulnasoft.oss.ksbulk.workflow.api.config.ConfigPostProcessor;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Console;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConfigPostProcessor} that detects missing password settings and attempts to prompt for
 * them, if standard input is available.
 */
public class PasswordPrompter implements ConfigPostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PasswordPrompter.class);

  private static final String DEFAULT_ENABLEMENT_PATH = "ksbulk.runner.promptForPasswords";

  private static final Map<String, String> DEFAULT_PATHS_TO_CHECK =
      ImmutableMap.<String, String>builder()
          // Deprecated KSBulk Driver paths
          .put("ksbulk.driver.auth.username", "ksbulk.driver.auth.password")
          .put("ksbulk.driver.ssl.trustore.path", "ksbulk.driver.ssl.trustore.password")
          .put("ksbulk.driver.ssl.keystore.path", "ksbulk.driver.ssl.keystore.password")
          // New Driver paths
          .put(
              "khulnasoft-java-driver.advanced.auth-provider.username",
              "khulnasoft-java-driver.advanced.auth-provider.password")
          .put(
              "khulnasoft-java-driver.advanced.ssl-engine-factory.truststore-path",
              "khulnasoft-java-driver.advanced.ssl-engine-factory.truststore-password")
          .put(
              "khulnasoft-java-driver.advanced.ssl-engine-factory.keystore-path",
              "khulnasoft-java-driver.advanced.ssl-engine-factory.keystore-password")
          // KSBulk paths
          .put(
              "ksbulk.monitoring.prometheus.push.username",
              "ksbulk.monitoring.prometheus.push.password")
          .build();

  private final Map<String, String> pathsToCheck;
  private final Console console;
  private final Function<Config, Boolean> enablementSupplier;

  @SuppressWarnings("unused")
  public PasswordPrompter() {
    this(
        DEFAULT_PATHS_TO_CHECK,
        System.console(),
        config -> config.getBoolean(DEFAULT_ENABLEMENT_PATH));
  }

  @VisibleForTesting
  PasswordPrompter(
      @NonNull Map<String, String> pathsToCheck,
      @Nullable Console console,
      @NonNull Function<Config, Boolean> enablementSupplier) {
    this.pathsToCheck = ImmutableMap.copyOf(pathsToCheck);
    this.console = console;
    this.enablementSupplier = enablementSupplier;
  }

  @Override
  public @NonNull Config postProcess(@NonNull Config config) {
    boolean passwordPromptingEnabled = enablementSupplier.apply(config);
    if (passwordPromptingEnabled) {
      if (console != null) {
        for (Entry<String, String> entry : pathsToCheck.entrySet()) {
          String pathToCheck = entry.getKey();
          if (ConfigUtils.isPathPresentAndNotEmpty(config, pathToCheck)) {
            String pathToPrompt = entry.getValue();
            if (!config.hasPath(pathToPrompt)) {
              config = ConfigUtils.readPassword(config, pathToPrompt, console);
            }
          }
        }
      } else {
        LOGGER.debug("Standard input not available.");
      }
    } else {
      LOGGER.debug("Password prompting disabled in configuration.");
    }
    return config;
  }
}
