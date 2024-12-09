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
package com.khulnasoft.oss.ksbulk.workflow.unload;

import com.khulnasoft.oss.ksbulk.workflow.api.Workflow;
import com.khulnasoft.oss.ksbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

public class UnloadWorkflowProvider implements WorkflowProvider {

  @NonNull
  @Override
  public String getTitle() {
    return "unload";
  }

  @NonNull
  @Override
  public String getDescription() {
    return "Unloads data from KhulnaSoft Enterprise or "
        + "Apache Cassandra (R) databases into external data sinks. "
        + "This command requires a connector to write data to; "
        + "the source table, or alternatively, the read query must also be properly configured. "
        + "Run `ksbulk help connector` or `ksbulk help schema` for more information.";
  }

  @NonNull
  @Override
  public Workflow newWorkflow(@NonNull Config config) {
    return new UnloadWorkflow(config);
  }
}
