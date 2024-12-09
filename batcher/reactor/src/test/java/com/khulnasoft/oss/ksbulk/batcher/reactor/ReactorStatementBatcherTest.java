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
package com.khulnasoft.oss.ksbulk.batcher.reactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.khulnasoft.oss.driver.api.core.cql.BatchStatement;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.metadata.Metadata;
import com.khulnasoft.oss.driver.api.core.metadata.TokenMap;
import com.khulnasoft.oss.ksbulk.batcher.api.BatchMode;
import com.khulnasoft.oss.ksbulk.batcher.api.StatementBatcherTest;
import java.util.HashSet;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class ReactorStatementBatcherTest extends StatementBatcherTest {

  @Test
  void should_batch_by_routing_key_reactive() {
    assignRoutingKeys();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher();
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token_reactive() {
    assignRoutingTokens();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher();
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_key_reactive() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(session, BatchMode.REPLICA_SET);
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_token_reactive() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(session, BatchMode.REPLICA_SET);
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available_reactive() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(session, BatchMode.REPLICA_SET);
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available_reactive() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(session, BatchMode.REPLICA_SET);
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_all_reactive() {
    ReactorStatementBatcher batcher = new ReactorStatementBatcher();
    Flux<Statement<?>> statements =
        Flux.from(batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(((BatchStatement) statements.blockFirst()))
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  void should_honor_max_batch_statements_reactive() {
    assignRoutingTokens();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(2);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_honor_max_size_in_bytes_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(8L);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_high_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(1000);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }

  @Test
  void should_buffer_by_max_size_in_bytes_if_satisfied_before_max_batch_statements_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(10, 8L);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_buffer_by_max_batch_statements_if_satisfied_before_max_size_in_bytes_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(1, 8L);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1), tuple(stmt2), tuple(stmt3), tuple(stmt4), tuple(stmt5), tuple(stmt6));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1), tuple(stmt2), tuple(stmt3), tuple(stmt4), tuple(stmt5), tuple(stmt6));
  }

  @Test
  void
      should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_high_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(100, 1000);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }

  @Test
  void
      should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_negative_reactive() {
    assignRoutingTokensWitSize();
    ReactorStatementBatcher batcher = new ReactorStatementBatcher(-1, -1);
    Flux<Statement<?>> statements =
        batcher.batchByGroupingKey(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
    assertThat(statements.collectList().block())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }
}
