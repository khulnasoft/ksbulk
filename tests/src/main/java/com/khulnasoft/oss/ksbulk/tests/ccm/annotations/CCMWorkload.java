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
package com.khulnasoft.oss.ksbulk.tests.ccm.annotations;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster.Workload;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** A set of workloads to assign to a specific node. */
@Retention(RUNTIME)
@Target(ANNOTATION_TYPE)
public @interface CCMWorkload {

  /**
   * The workloads to assign to a specific node.
   *
   * @return The workloads to assign to a specifc node.
   */
  Workload[] value() default {};
}
