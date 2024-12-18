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

import com.khulnasoft.oss.driver.api.core.config.DefaultDriverOption;
import com.khulnasoft.oss.driver.api.core.config.DriverOption;
import com.khulnasoft.oss.ksbulk.workflow.commons.policies.lbp.SimpleNodeDistanceEvaluator;
import com.khulnasoft.oss.ksbulk.workflow.commons.policies.retry.MultipleRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Driver options that are defined by KSBulk. */
public enum BulkDriverOption implements DriverOption {

  /**
   * The default port to use when {@link DefaultDriverOption#CONTACT_POINTS} contains hosts without
   * ports.
   *
   * <p>Expected type: int.
   */
  DEFAULT_PORT("basic.default-port"),

  /**
   * The maximum number of attempts to retry, when using {@link MultipleRetryPolicy}.
   *
   * <p>Expected type: int.
   */
  RETRY_POLICY_MAX_RETRIES("advanced.retry-policy.max-retries"),

  /**
   * The list of allowed nodes, for use when {@link SimpleNodeDistanceEvaluator} is used.
   *
   * <p>Expected type: String List.
   */
  LOAD_BALANCING_POLICY_FILTER_ALLOW("basic.load-balancing-policy.evaluator.allow"),

  /**
   * The list of denied nodes, for use when {@link SimpleNodeDistanceEvaluator} is used.
   *
   * <p>Expected type: String List.
   */
  LOAD_BALANCING_POLICY_FILTER_DENY("basic.load-balancing-policy.evaluator.deny"),
  ;

  private final String path;

  BulkDriverOption(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }
}
