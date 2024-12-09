/*
 * Copyright KhulnaSoft, Inc.
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
package com.khulnasoft.oss.ksbulk.workflow.commons.auth;

import com.khulnasoft.dse.driver.api.core.auth.DseGssApiAuthProviderBase;
import com.khulnasoft.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;

public class BulkGssApiAuthProvider extends DseGssApiAuthProviderBase {

  private final GssApiOptions options;

  public BulkGssApiAuthProvider(GssApiOptions options) {
    super("");
    this.options = options;
  }

  @NonNull
  @Override
  protected GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return options;
  }
}
