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

import com.khulnasoft.oss.ksbulk.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Optional;
import java.util.OptionalInt;

public class EngineSettings {

  private static final String DRY_RUN = "dryRun";
  private static final String EXECUTION_ID = "executionId";
  private static final String MAX_CONCURRENT_QUERIES = "maxConcurrentQueries";
  private static final String DATA_SIZE_SAMPLING_ENABLED = "dataSizeSamplingEnabled";

  private final Config config;

  private boolean dryRun;
  private String executionId;
  private int maxConcurrentQueries;
  private boolean dataSizeSamplingEnabled;

  EngineSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {
      dryRun = config.getBoolean(DRY_RUN);
      executionId = config.hasPath(EXECUTION_ID) ? config.getString(EXECUTION_ID) : null;
      maxConcurrentQueries =
          config.getString(MAX_CONCURRENT_QUERIES).equalsIgnoreCase("AUTO")
              ? -1
              : ConfigUtils.getThreads(config, MAX_CONCURRENT_QUERIES);
      dataSizeSamplingEnabled = config.getBoolean(DATA_SIZE_SAMPLING_ENABLED);
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "ksbulk.engine");
    }
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public Optional<String> getCustomExecutionIdTemplate() {
    return Optional.ofNullable(executionId);
  }

  public OptionalInt getMaxConcurrentQueries() {
    return maxConcurrentQueries == -1 ? OptionalInt.empty() : OptionalInt.of(maxConcurrentQueries);
  }

  public boolean isDataSizeSamplingEnabled() {
    return dataSizeSamplingEnabled;
  }
}
