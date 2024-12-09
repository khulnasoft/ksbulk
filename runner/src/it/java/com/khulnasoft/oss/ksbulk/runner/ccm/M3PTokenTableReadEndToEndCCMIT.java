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
package com.khulnasoft.oss.ksbulk.runner.ccm;

import static com.khulnasoft.oss.ksbulk.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDERR;
import static com.khulnasoft.oss.ksbulk.tests.logging.StreamType.STDOUT;
import static org.slf4j.event.Level.INFO;

import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.driver.api.core.metadata.token.Token;
import com.khulnasoft.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMConfig;
import com.khulnasoft.oss.ksbulk.tests.driver.annotations.SessionConfig;
import com.khulnasoft.oss.ksbulk.tests.logging.LogCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamCapture;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import org.junit.jupiter.api.Tag;

@CCMConfig(numberOfNodes = 3)
@Tag("long")
class M3PTokenTableReadEndToEndCCMIT extends TableReadEndToEndCCMITBase {

  M3PTokenTableReadEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(useKeyspace = NONE) CqlSession session,
      @LogCapture(level = INFO, loggerName = "com.khulnasoft.oss.ksbulk") LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdout,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session, logs, stdout, stderr);
  }

  @Override
  Token getMinToken() {
    return Murmur3TokenFactory.MIN_TOKEN;
  }

  @Override
  Token getMaxToken() {
    return Murmur3TokenFactory.MAX_TOKEN;
  }
}
