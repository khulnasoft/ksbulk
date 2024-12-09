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
package com.khulnasoft.oss.ksbulk.executor.reactor.ccm;

import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.ksbulk.tests.driver.SerializedSession;
import com.khulnasoft.oss.ksbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.khulnasoft.oss.ksbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster;
import com.khulnasoft.oss.ksbulk.tests.ccm.CCMCluster.Type;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMRequirements;
import com.khulnasoft.oss.ksbulk.tests.ccm.annotations.CCMVersionRequirement;
import org.junit.jupiter.api.Tag;

@Tag("medium")
@CCMRequirements(
    compatibleTypes = {Type.DSE},
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "5.1"))
class ContinuousReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousReactorBulkExecutorCCMIT(CCMCluster ccm, CqlSession session) {
    super(
        ccm,
        session,
        ContinuousReactorBulkExecutor.continuousPagingBuilder(new SerializedSession(session))
            .build(),
        ContinuousReactorBulkExecutor.continuousPagingBuilder(new SerializedSession(session))
            .failSafe()
            .build());
  }
}
