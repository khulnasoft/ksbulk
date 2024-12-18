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
package com.khulnasoft.oss.ksbulk.executor.api.subscription;

import com.khulnasoft.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.detach.AttachmentPoint;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.AbstractIterator;
import com.khulnasoft.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.khulnasoft.oss.ksbulk.executor.api.exception.BulkExecutionException;
import com.khulnasoft.oss.ksbulk.executor.api.listener.ExecutionContext;
import com.khulnasoft.oss.ksbulk.executor.api.listener.ExecutionListener;
import com.khulnasoft.oss.ksbulk.executor.api.result.DefaultReadResult;
import com.khulnasoft.oss.ksbulk.executor.api.result.ReadResult;
import com.khulnasoft.oss.ksbulk.sampler.DataSizes;
import com.khulnasoft.oss.ksbulk.sampler.SizeableRow;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Subscriber;

public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, ContinuousAsyncResultSet> {

  private final AtomicLong position = new AtomicLong(0);

  public ContinuousReadResultSubscription(
      @NonNull Subscriber<? super ReadResult> subscriber,
      @NonNull Statement<?> statement,
      @NonNull AttachmentPoint attachmentPoint,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable RateLimiter rateLimiter,
      @Nullable RateLimiter bytesRateLimiter,
      boolean failFast) {
    super(
        subscriber,
        statement,
        attachmentPoint,
        listener,
        maxConcurrentRequests,
        rateLimiter,
        bytesRateLimiter,
        failFast);
  }

  @Override
  Page toPage(ContinuousAsyncResultSet rs, ExecutionContext local) {
    Iterator<Row> rows = rs.currentPage().iterator();
    Iterator<ReadResult> results =
        new AbstractIterator<ReadResult>() {

          @Override
          protected ReadResult computeNext() {
            if (rows.hasNext()) {
              Row row = new SizeableRow(rows.next());
              if (listener != null) {
                listener.onRowReceived(row, local);
              }
              return new DefaultReadResult(
                  statement, rs.getExecutionInfo(), row, position.incrementAndGet());
            }
            return endOfData();
          }
        };
    return new ContinuousPage(rs, results);
  }

  @Override
  public void cancel() {
    Page current = pages.peek();
    if (current instanceof ContinuousPage) {
      // forcibly cancel the continuous paging request
      ((ContinuousPage) current).rs.cancel();
    }
    super.cancel();
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestStarted(statement, local);
    }
  }

  @Override
  void onRequestSuccessful(ContinuousAsyncResultSet page, ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestSuccessful(statement, local);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestFailed(statement, t, local);
    }
  }

  @Override
  void onBeforeResultEmitted(ReadResult result) {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
    if (bytesRateLimiter != null && result.getRow().isPresent()) {
      long dataSize = DataSizes.getDataSize(result.getRow().get());
      bytesRateLimiter.acquire((int) dataSize);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }

  private class ContinuousPage extends Page {

    final ContinuousAsyncResultSet rs;

    private ContinuousPage(ContinuousAsyncResultSet rs, Iterator<ReadResult> rows) {
      super(rows, rs.hasMorePages() ? rs::fetchNextPage : null);
      this.rs = rs;
    }
  }
}
