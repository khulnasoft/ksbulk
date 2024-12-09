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
package com.khulnasoft.oss.ksbulk.runner.cli;

import edu.umd.cs.findbugs.annotations.Nullable;

/** Simple exception indicating that the user wants the main help output. */
public class GlobalHelpRequestException extends Exception {

  private final String connectorName;

  GlobalHelpRequestException() {
    this(null);
  }

  GlobalHelpRequestException(@Nullable String connectorName) {
    this.connectorName = connectorName;
  }

  @Nullable
  public String getConnectorName() {
    return connectorName;
  }
}
