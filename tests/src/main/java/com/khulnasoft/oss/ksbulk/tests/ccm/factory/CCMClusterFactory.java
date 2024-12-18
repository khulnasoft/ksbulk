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
package com.khulnasoft.oss.ksbulk.tests.ccm.factory;

import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster.Workload;
import com.khulnasoft.oss.ksbulk.tests.ccm.DefaultCCMCluster;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMConfig;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMFactory;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMFactoryMethod;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMWorkload;
import com.khulnasoft.oss.ksbulk.tests.utils.ReflectionUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CCMClusterFactory {

  @CCMConfig
  private static class Dummy {}

  private static final CCMConfig DEFAULT_CCM_CONFIG = Dummy.class.getAnnotation(CCMConfig.class);

  private CCMClusterFactory() {}

  public static CCMClusterFactory createInstanceForClass(Class<?> testClass) {
    CCMFactoryMethod factoryRef =
        ReflectionUtils.locateClassAnnotation(testClass, CCMFactoryMethod.class);
    CCMConfig config = ReflectionUtils.locateClassAnnotation(testClass, CCMConfig.class);
    if (factoryRef != null) {
      if (config != null) {
        throw new IllegalStateException(
            String.format(
                "%s can be annotated with either @CCMConfig or @CCMFactoryMethod, but not both",
                testClass));
      }
      return new CCMClusterMethodFactory(factoryRef, testClass);
    }
    if (config == null) {
      config = DEFAULT_CCM_CONFIG;
    }
    return new CCMClusterAnnotationFactory(config);
  }

  public abstract DefaultCCMCluster.Builder createCCMClusterBuilder();

  private static class CCMClusterAnnotationFactory extends CCMClusterFactory {

    private final int[] numberOfNodes;
    private final boolean ssl;
    private final boolean hostnameVerification;
    private final boolean auth;
    private final Map<String, Object> cassandraConfig;
    private final Map<String, Object> dseConfig;
    private final Set<String> jvmArgs;
    private final Set<String> createOptions;
    private final List<Workload[]> workloads;

    private CCMClusterAnnotationFactory(CCMConfig config) {
      this.numberOfNodes = config.numberOfNodes();
      this.ssl = config.ssl();
      this.hostnameVerification = config.hostnameVerification();
      this.auth = config.auth();
      this.cassandraConfig = toConfigMap(config.config());
      this.dseConfig = toConfigMap(config.dseConfig());
      this.jvmArgs = toConfigSet(config.jvmArgs());
      this.createOptions = toConfigSet(config.createOptions());
      this.workloads = computeWorkloads(config);
    }

    private static Map<String, Object> toConfigMap(String[] conf) {
      Map<String, Object> config = new HashMap<>();
      for (String aConf : conf) {
        @SuppressWarnings("StringSplitter")
        String[] tokens = aConf.split(":");
        if (tokens.length != 2) {
          throw new IllegalArgumentException("Wrong configuration option: " + aConf);
        }
        String key = tokens[0];
        String value = tokens[1];
        config.put(key, value);
      }
      return config;
    }

    private static Set<String> toConfigSet(String[] config) {
      Set<String> args = new LinkedHashSet<>();
      Collections.addAll(args, config);
      return args;
    }

    private static List<Workload[]> computeWorkloads(CCMConfig config) {
      int total = 0;
      for (int perDc : config.numberOfNodes()) {
        total += perDc;
      }
      List<Workload[]> workloads = new ArrayList<>(Collections.nCopies(total, null));
      CCMWorkload[] annWorkloads = config.workloads();
      for (int i = 0; i < annWorkloads.length; i++) {
        CCMWorkload nodeWorkloads = annWorkloads[i];
        workloads.set(i, nodeWorkloads.value());
      }
      return workloads;
    }

    @Override
    public DefaultCCMCluster.Builder createCCMClusterBuilder() {
      DefaultCCMCluster.Builder ccmBuilder = DefaultCCMCluster.builder().withNodes(numberOfNodes);
      if (ssl || auth) {
        ccmBuilder.withSSL(hostnameVerification);
      }
      if (auth) {
        ccmBuilder.withAuth();
      }
      for (Map.Entry<String, Object> entry : cassandraConfig.entrySet()) {
        ccmBuilder.withCassandraConfiguration(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<String, Object> entry : dseConfig.entrySet()) {
        ccmBuilder.withDSEConfiguration(entry.getKey(), entry.getValue());
      }
      for (String option : createOptions) {
        ccmBuilder.withCreateOptions(option);
      }
      for (String arg : jvmArgs) {
        ccmBuilder.withJvmArgs(arg);
      }
      for (int i = 0; i < workloads.size(); i++) {
        Workload[] workload = workloads.get(i);
        if (workload != null) {
          ccmBuilder.withWorkload(i + 1, workload);
        }
      }
      return ccmBuilder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CCMClusterAnnotationFactory)) {
        return false;
      }
      CCMClusterAnnotationFactory that = (CCMClusterAnnotationFactory) o;
      return ssl == that.ssl
          && auth == that.auth
          && Arrays.equals(numberOfNodes, that.numberOfNodes)
          && cassandraConfig.equals(that.cassandraConfig)
          && dseConfig.equals(that.dseConfig)
          && jvmArgs.equals(that.jvmArgs)
          && createOptions.equals(that.createOptions)
          && workloads.equals(that.workloads);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(numberOfNodes);
      result = 31 * result + (ssl ? 1 : 0);
      result = 31 * result + (auth ? 1 : 0);
      result = 31 * result + cassandraConfig.hashCode();
      result = 31 * result + dseConfig.hashCode();
      result = 31 * result + jvmArgs.hashCode();
      result = 31 * result + createOptions.hashCode();
      result = 31 * result + workloads.hashCode();
      return result;
    }
  }

  private static class CCMClusterMethodFactory extends CCMClusterFactory {

    private final Method factoryMethod;

    private CCMClusterMethodFactory(CCMFactoryMethod factoryRef, Class<?> testClass) {
      factoryMethod = locateCCMFactoryMethod(factoryRef, testClass);
    }

    private static Method locateCCMFactoryMethod(CCMFactoryMethod factoryRef, Class<?> testClass) {
      String methodName = factoryRef.value();
      Class<?> factoryClass =
          factoryRef.factoryClass().equals(CCMFactoryMethod.TestClass.class)
              ? testClass
              : factoryRef.factoryClass();
      Method method = ReflectionUtils.locateMethod(methodName, factoryClass, 0);
      if (method == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find factory method %s in %s", methodName, factoryClass));
      }
      if (method.getAnnotation(CCMFactory.class) == null) {
        throw new IllegalArgumentException(
            String.format("Method %s must be annotated with @CCMFactory", method));
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new IllegalArgumentException(String.format("Method %s must be static", method));
      }
      return method;
    }

    @Override
    public DefaultCCMCluster.Builder createCCMClusterBuilder() {
      DefaultCCMCluster.Builder ccmBuilder =
          ReflectionUtils.invokeMethod(factoryMethod, null, DefaultCCMCluster.Builder.class);
      if (ccmBuilder == null) {
        throw new NullPointerException(
            String.format("CCM factory method %s returned null", factoryMethod));
      }
      return ccmBuilder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CCMClusterMethodFactory)) {
        return false;
      }
      CCMClusterMethodFactory that = (CCMClusterMethodFactory) o;
      return factoryMethod.equals(that.factoryMethod);
    }

    @Override
    public int hashCode() {
      return factoryMethod.hashCode();
    }
  }
}
