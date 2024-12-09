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
package com.khulnasoft.oss.ksbulk.executor.api.reader;

import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.khulnasoft.oss.ksbulk.executor.api.exception.BulkExecutionException;
import com.khulnasoft.oss.ksbulk.executor.api.result.ReadResult;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;

/**
 * A bulk reader that operates in 3 distinct modes:
 *
 * <ol>
 *   <li>{@link SyncBulkReader Synchronous};
 *   <li>{@link AsyncBulkReader Asynchronous};
 *   <li>{@link ReactiveBulkReader Reactive}.
 * </ol>
 */
public interface BulkReader extends SyncBulkReader, AsyncBulkReader, ReactiveBulkReader {

  @Override
  default void readSync(Statement<?> statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    try {
      Uninterruptibles.getUninterruptibly(readAsync(statement, consumer));
    } catch (ExecutionException e) {
      throw ((BulkExecutionException) e.getCause());
    }
  }

  @Override
  default void readSync(
      Publisher<? extends Statement<?>> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    try {
      Uninterruptibles.getUninterruptibly(readAsync(statements, consumer));
    } catch (ExecutionException e) {
      throw ((BulkExecutionException) e.getCause());
    }
  }
}
