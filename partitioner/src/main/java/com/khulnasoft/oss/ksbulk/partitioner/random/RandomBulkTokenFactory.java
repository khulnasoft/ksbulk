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
package com.khulnasoft.oss.ksbulk.partitioner.random;

import com.khulnasoft.oss.driver.api.core.metadata.EndPoint;
import com.khulnasoft.oss.driver.api.core.metadata.token.Token;
import com.khulnasoft.oss.driver.internal.core.metadata.token.RandomToken;
import com.khulnasoft.oss.driver.internal.core.metadata.token.RandomTokenFactory;
import com.khulnasoft.oss.ksbulk.partitioner.BulkTokenFactory;
import com.khulnasoft.oss.ksbulk.partitioner.BulkTokenRange;
import com.khulnasoft.oss.ksbulk.partitioner.TokenRangeClusterer;
import com.khulnasoft.oss.ksbulk.partitioner.TokenRangeSplitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.Set;

/** A {@link BulkTokenFactory} for the Random Partitioner. */
public class RandomBulkTokenFactory extends RandomTokenFactory implements BulkTokenFactory {

  public static final BigInteger TOTAL_TOKEN_COUNT =
      MAX_TOKEN.getValue().subtract(MIN_TOKEN.getValue());

  @NonNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NonNull
  @Override
  public BulkTokenRange range(
      @NonNull Token start, @NonNull Token end, @NonNull Set<EndPoint> replicas) {
    return new RandomBulkTokenRange(((RandomToken) start), (RandomToken) end, replicas);
  }

  @NonNull
  @Override
  public TokenRangeSplitter splitter() {
    return new RandomTokenRangeSplitter();
  }

  @NonNull
  @Override
  public TokenRangeClusterer clusterer() {
    return new TokenRangeClusterer(this);
  }
}
