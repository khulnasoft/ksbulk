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
package com.khulnasoft.oss.ksbulk.tests.simulacron;

import com.khulnasoft.oss.driver.api.core.metadata.EndPoint;
import com.khulnasoft.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.khulnasoft.oss.ksbulk.tests.RemoteClusterExtension;
import com.khulnasoft.oss.ksbulk.tests.simulacron.factory.BoundClusterFactory;
import com.khulnasoft.oss.ksbulk.tests.utils.NetworkUtils;
import com.khulnasoft.oss.simulacron.server.BoundCluster;
import com.khulnasoft.oss.simulacron.server.BoundNode;
import com.khulnasoft.oss.simulacron.server.Inet4Resolver;
import com.khulnasoft.oss.simulacron.server.Server;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manager for {@link BoundCluster Simulacron} clusters that helps testing with JUnit 5 and
 * Cassandra.
 */
public class SimulacronExtension extends RemoteClusterExtension implements AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimulacronExtension.class);
  private static final String SIMULACRON = "SIMULACRON";
  private static final Server SERVER =
      Server.builder()
          .withAddressResolver(new Inet4Resolver(NetworkUtils.findAvailablePort()))
          .build();

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.equals(BoundCluster.class)
        || super.supportsParameter(parameterContext, extensionContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (BoundCluster.class.equals(type)) {
      BoundCluster boundCluster = getOrCreateBoundCluster(extensionContext);
      LOGGER.debug(String.format("Returning %s for parameter %s", boundCluster, parameter));
      return boundCluster;
    } else {
      return super.resolveParameter(parameterContext, extensionContext);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    BoundCluster boundCluster = getOrCreateBoundCluster(context);
    boundCluster.clearLogs();
    boundCluster.clearPrimes(true);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    super.afterAll(context);
    stopBoundCluster(context);
  }

  @Override
  protected List<EndPoint> getContactPoints(ExtensionContext context) {
    return getOrCreateBoundCluster(context).dc(0).getNodes().stream()
        .map(BoundNode::inetSocketAddress)
        .map(DefaultEndPoint::new)
        .collect(Collectors.toList());
  }

  @Override
  protected String getLocalDatacenter(ExtensionContext context) {
    return getOrCreateBoundCluster(context).dc(0).getName();
  }

  private BoundCluster getOrCreateBoundCluster(ExtensionContext context) {
    return context
        .getStore(TEST_NAMESPACE)
        .getOrComputeIfAbsent(
            SIMULACRON,
            f -> {
              BoundClusterFactory factory =
                  BoundClusterFactory.createInstanceForClass(context.getRequiredTestClass());
              BoundCluster boundCluster = SERVER.register(factory.createClusterSpec());
              boundCluster.start();
              SimulacronUtils.primeSystemLocal(boundCluster, Collections.emptyMap());
              SimulacronUtils.primeSystemPeers(boundCluster);
              SimulacronUtils.primeSystemPeersV2(boundCluster);
              return boundCluster;
            },
            BoundCluster.class);
  }

  private void stopBoundCluster(ExtensionContext context) {
    BoundCluster boundCluster =
        context.getStore(TEST_NAMESPACE).remove(SIMULACRON, BoundCluster.class);
    if (boundCluster != null) {
      boundCluster.stop();
    }
  }
}
