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
package com.khulnasoft.oss.ksbulk.batcher.reactor;

import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.driver.api.core.cql.BatchType;
import com.khulnasoft.oss.ksbulk.batcher.api.BatchMode;
import com.khulnasoft.oss.ksbulk.batcher.api.ReactiveStatementBatcher;
import com.khulnasoft.oss.ksbulk.batcher.api.ReactiveStatementBatcherFactory;
import edu.umd.cs.findbugs.annotations.NonNull;

public class ReactorStatementBatcherFactory implements ReactiveStatementBatcherFactory {

  @Override
  public ReactiveStatementBatcher create() {
    return new ReactorStatementBatcher();
  }

  @Override
  public ReactiveStatementBatcher create(int maxBatchStatements) {
    return new ReactorStatementBatcher(maxBatchStatements);
  }

  @Override
  public ReactiveStatementBatcher create(long maxSizeInBytes) {
    return new ReactorStatementBatcher(maxSizeInBytes);
  }

  @Override
  public ReactiveStatementBatcher create(int maxBatchStatements, long maxSizeInBytes) {
    return new ReactorStatementBatcher(maxBatchStatements, maxSizeInBytes);
  }

  @Override
  public ReactiveStatementBatcher create(
      @NonNull CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    return new ReactorStatementBatcher(
        session, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
  }
}
