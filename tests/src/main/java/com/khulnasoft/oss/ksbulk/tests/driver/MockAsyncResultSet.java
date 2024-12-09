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
package com.khulnasoft.oss.ksbulk.tests.driver;

import com.khulnasoft.oss.driver.api.core.cql.AsyncResultSet;
import com.khulnasoft.oss.driver.api.core.cql.ColumnDefinitions;
import com.khulnasoft.oss.driver.api.core.cql.ExecutionInfo;
import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class MockAsyncResultSet implements AsyncResultSet {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final CompletionStage<AsyncResultSet> nextPage;
  private final ExecutionInfo executionInfo;
  private int remaining;

  public MockAsyncResultSet(
      int size, ExecutionInfo executionInfo, CompletionStage<AsyncResultSet> nextPage) {
    rows = IntStream.range(0, size).boxed().map(MockRow::new).collect(Collectors.toList());
    this.executionInfo = executionInfo;
    iterator = rows.iterator();
    remaining = size;
    this.nextPage = nextPage;
  }

  @Override
  public Row one() {
    if (!iterator.hasNext()) {
      return null;
    }
    Row next = iterator.next();
    remaining--;
    return next;
  }

  @Override
  @NonNull
  public Iterable<Row> currentPage() {
    return rows;
  }

  @Override
  @NonNull
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    return nextPage;
  }

  @Override
  public int remaining() {
    return remaining;
  }

  @Override
  public boolean hasMorePages() {
    return nextPage != null;
  }

  @Override
  @NonNull
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  @NonNull
  public ColumnDefinitions getColumnDefinitions() {
    return EmptyColumnDefinitions.INSTANCE;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
