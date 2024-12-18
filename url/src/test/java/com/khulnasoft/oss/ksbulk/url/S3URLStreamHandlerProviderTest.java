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
package com.khulnasoft.oss.ksbulk.url;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class S3URLStreamHandlerProviderTest {

  @Test
  void should_handle_s3_protocol() {
    Config config = mock(Config.class);
    when(config.hasPath("ksbulk.s3.clientCacheSize")).thenReturn(true);
    when(config.getInt("ksbulk.s3.clientCacheSize")).thenReturn(25);

    S3URLStreamHandlerProvider provider = new S3URLStreamHandlerProvider();

    assertThat(provider.maybeCreateURLStreamHandler("s3", config))
        .isNotNull()
        .containsInstanceOf(S3URLStreamHandler.class);
  }
}
