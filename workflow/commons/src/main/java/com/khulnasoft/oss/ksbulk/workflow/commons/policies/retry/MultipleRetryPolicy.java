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
package com.khulnasoft.oss.ksbulk.workflow.commons.policies.retry;

import com.khulnasoft.oss.driver.api.core.ConsistencyLevel;
import com.khulnasoft.oss.driver.api.core.connection.ClosedConnectionException;
import com.khulnasoft.oss.driver.api.core.connection.HeartbeatException;
import com.khulnasoft.oss.driver.api.core.context.DriverContext;
import com.khulnasoft.oss.driver.api.core.retry.RetryDecision;
import com.khulnasoft.oss.driver.api.core.retry.RetryPolicy;
import com.khulnasoft.oss.driver.api.core.servererrors.CoordinatorException;
import com.khulnasoft.oss.driver.api.core.servererrors.ReadFailureException;
import com.khulnasoft.oss.driver.api.core.servererrors.WriteFailureException;
import com.khulnasoft.oss.driver.api.core.servererrors.WriteType;
import com.khulnasoft.oss.driver.api.core.session.Request;
import com.khulnasoft.oss.ksbulk.workflow.commons.settings.BulkDriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;

public class MultipleRetryPolicy implements RetryPolicy {

  private final int maxRetryCount;

  public MultipleRetryPolicy(DriverContext context, String profileName) {
    this.maxRetryCount =
        context
            .getConfig()
            .getProfile(profileName)
            .getInt(BulkDriverOption.RETRY_POLICY_MAX_RETRIES, 10);
  }

  @Override
  @Deprecated
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;
  }

  @Override
  @Deprecated
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;
  }

  @Override
  @Deprecated
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;
  }

  @Override
  @Deprecated
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    return (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
        ? RetryDecision.RETRY_NEXT
        : RetryDecision.RETHROW;
  }

  @Override
  @Deprecated
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    return (error instanceof ReadFailureException || error instanceof WriteFailureException)
        ? RetryDecision.RETHROW
        : RetryDecision.RETRY_NEXT;
  }

  @Override
  public void close() {}
}
