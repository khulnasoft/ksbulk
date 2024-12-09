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
package com.khulnasoft.oss.ksbulk.tests.cloud;

import com.khulnasoft.oss.driver.api.core.metadata.EndPoint;
import com.khulnasoft.oss.driver.internal.core.config.cloud.CloudConfig;
import com.khulnasoft.oss.driver.internal.core.config.cloud.CloudConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSNIProxyServer implements SNIProxyServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSNIProxyServer.class);

  private static final String PROXY_PATH = "ksbulk.cloud.PROXY_PATH";

  private final Path proxyPath;

  private volatile boolean running = false;
  private volatile CloudConfig config;

  public DefaultSNIProxyServer() {
    this(Paths.get(System.getProperty(PROXY_PATH, "./")));
  }

  public DefaultSNIProxyServer(@NonNull Path proxyPath) {
    this.proxyPath = proxyPath.toAbsolutePath();
  }

  @Override
  public void start() {
    CommandLine run = CommandLine.parse(proxyPath.resolve("run.sh").toString());
    execute(run);
    running = true;
    try {
      config =
          new CloudConfigFactory().createCloudConfig(Files.newInputStream(getSecureBundlePath()));
    } catch (IOException | GeneralSecurityException e) {
      // should never happen, the bundle is always present and readable once the proxy is started
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (running) {
      CommandLine findImageId =
          CommandLine.parse("docker ps -a -q --filter ancestor=single_endpoint");
      String id = execute(findImageId);
      CommandLine stop = CommandLine.parse("docker kill " + id);
      execute(stop);
      running = false;
    }
  }

  @Override
  public List<EndPoint> getContactPoints() {
    return config.getEndPoints();
  }

  @Override
  public String getLocalDatacenter() {
    return config.getLocalDatacenter();
  }

  @Override
  public Path getSecureBundlePath() {
    // Bundles currently available as of 2019-10:
    // creds-v1-invalid-ca.zip
    // creds-v1-unreachable.zip
    // creds-v1-wo-cert.zip
    // creds-v1-wo-creds.zip
    // creds-v1.zip
    // Use the bundle without credentials, as this is the typical bundle currently in use.
    return proxyPath.resolve("certs/bundles/creds-v1-wo-creds.zip");
  }

  private String execute(CommandLine cli) {
    LOGGER.debug("Executing: " + cli);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    try (StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        LogOutputStream outStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOGGER.debug("sniendpointout> {}", line);
                pw.println(line);
              }
            };
        LogOutputStream errStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOGGER.error("sniendpointerr> {}", line);
              }
            }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);
      executor.setWorkingDirectory(proxyPath.toFile());
      int retValue = executor.execute(cli);
      if (retValue != 0) {
        LOGGER.error(
            "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli);
      }
      return sw.toString();
    } catch (IOException ex) {
      if (watchDog.killedProcess()) {
        throw new RuntimeException("The command '" + cli + "' was killed after 10 minutes");
      } else {
        throw new RuntimeException("The command '" + cli + "' failed to execute", ex);
      }
    }
  }
}
