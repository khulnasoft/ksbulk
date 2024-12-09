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
package com.khulnasoft.oss.ksbulk.runner.simulacron;

import static java.nio.file.Files.createTempDirectory;

import com.khulnasoft.oss.ksbulk.tests.logging.LogConfigurationResource;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.LogInterceptor;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptingExtension;
import com.khulnasoft.oss.ksbulk.tests.logging.StreamInterceptor;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronExtension;
import com.khulnasoft.oss.ksbulk.tests.simulacron.SimulacronUtils;
import com.khulnasoft.oss.ksbulk.tests.utils.FileUtils;
import com.khulnasoft.oss.ksbulk.tests.utils.StringUtils;
import com.khulnasoft.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@LogConfigurationResource("logback.xml")
class EndToEndSimulacronITBase {

  final BoundCluster simulacron;
  final LogInterceptor logs;
  final StreamInterceptor stdOut;
  final StreamInterceptor stdErr;
  final String hostname;
  final int port;

  Path unloadDir;
  Path logDir;

  EndToEndSimulacronITBase(
      BoundCluster simulacron,
      LogInterceptor logs,
      StreamInterceptor stdOut,
      StreamInterceptor stdErr) {
    this.simulacron = simulacron;
    this.logs = logs;
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    hostname = node.getAddress().getHostAddress();
    port = node.getPort();
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    simulacron.clearLogs();
    SimulacronUtils.primeSystemLocal(simulacron, Collections.emptyMap());
    SimulacronUtils.primeSystemPeers(simulacron);
    SimulacronUtils.primeSystemPeersV2(simulacron);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    FileUtils.deleteDirectory(logDir);
    FileUtils.deleteDirectory(unloadDir);
  }

  String[] addCommonSettings(String[] args) {
    String[] commonArgs =
        new String[] {
          "--log.directory",
          StringUtils.quoteJson(logDir),
          "-h",
          hostname,
          "-port",
          String.valueOf(port),
          "-dc",
          "dc1",
          "-cl",
          "LOCAL_ONE",
          "--driver.advanced.protocol.version",
          "V4",
          "--driver.advanced.connection.pool.local.size",
          "1",
        };
    return Stream.of(args, commonArgs).flatMap(Stream::of).toArray(String[]::new);
  }
}
