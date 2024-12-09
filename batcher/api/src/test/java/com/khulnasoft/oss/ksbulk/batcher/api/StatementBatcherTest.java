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
package com.khulnasoft.oss.ksbulk.batcher.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.khulnasoft.oss.driver.api.core.CqlIdentifier;
import com.khulnasoft.oss.driver.api.core.CqlSession;
import com.khulnasoft.oss.driver.api.core.ProtocolVersion;
import com.khulnasoft.oss.driver.api.core.context.DriverContext;
import com.khulnasoft.oss.driver.api.core.cql.BatchStatement;
import com.khulnasoft.oss.driver.api.core.cql.BatchableStatement;
import com.khulnasoft.oss.driver.api.core.cql.SimpleStatement;
import com.khulnasoft.oss.driver.api.core.cql.Statement;
import com.khulnasoft.oss.driver.api.core.data.ByteUtils;
import com.khulnasoft.oss.driver.api.core.metadata.Metadata;
import com.khulnasoft.oss.driver.api.core.metadata.Node;
import com.khulnasoft.oss.driver.api.core.metadata.TokenMap;
import com.khulnasoft.oss.driver.api.core.metadata.token.Token;
import com.khulnasoft.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.khulnasoft.oss.driver.shaded.guava.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatementBatcherTest {

  protected static final ThrowingExtractor<Statement<?>, Tuple, RuntimeException> EXTRACTOR =
      stmt -> {
        if (stmt instanceof SimpleStatement) {
          return tuple(stmt);
        } else {
          List<Statement<?>> children = new ArrayList<>();
          for (BatchableStatement<?> child : ((BatchStatement) stmt)) {
            children.add(child);
          }
          return tuple(children.toArray());
        }
      };

  protected final ByteBuffer key1 = ByteUtils.fromHexString("0x1234");
  protected final ByteBuffer key2 = ByteUtils.fromHexString("0x5678");
  protected final ByteBuffer key3 = ByteUtils.fromHexString("0x9abc");

  protected final Token token1 = mock(Token.class);
  protected final Token token2 = mock(Token.class);

  protected final CqlIdentifier ks = CqlIdentifier.fromInternal("ks");

  protected SimpleStatement stmt1 = SimpleStatement.newInstance("stmt1", "abcd").setKeyspace(ks);
  protected SimpleStatement stmt2 = SimpleStatement.newInstance("stmt2", "efgh").setKeyspace(ks);
  protected SimpleStatement stmt3 = SimpleStatement.newInstance("stmt3", "ijkl").setKeyspace(ks);
  protected SimpleStatement stmt4 = SimpleStatement.newInstance("stmt4", "jklm").setKeyspace(ks);
  protected SimpleStatement stmt5 = SimpleStatement.newInstance("stmt5", "klmn").setKeyspace(ks);
  protected SimpleStatement stmt6 = SimpleStatement.newInstance("stmt6", "lmno").setKeyspace(ks);

  protected CqlSession session;

  protected final Node node1 = mock(Node.class);
  protected final Node node2 = mock(Node.class);
  protected final Node node3 = mock(Node.class);
  protected final Node node4 = mock(Node.class);

  protected final Set<Node> replicaSet1 = Sets.newHashSet(node1, node2, node3);
  protected final Set<Node> replicaSet2 = Sets.newHashSet(node2, node3, node4);

  @BeforeEach
  void setUp() {
    session = mock(CqlSession.class);
    DriverContext context = mock(DriverContext.class);
    when(session.getContext()).thenReturn(context);
    when(context.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
  }

  @Test
  void should_batch_by_routing_key() {
    assignRoutingKeys();
    StatementBatcher batcher = new DefaultStatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token() {
    assignRoutingTokens();
    StatementBatcher batcher = new DefaultStatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_key() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new DefaultStatementBatcher(session, BatchMode.REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_token() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new DefaultStatementBatcher(session, BatchMode.REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new DefaultStatementBatcher(session, BatchMode.REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new DefaultStatementBatcher(session, BatchMode.REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_all() {
    StatementBatcher batcher = new DefaultStatementBatcher();
    List<Statement<?>> statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).hasSize(1);
    Statement<?> statement = statements.get(0);
    assertThat(((BatchStatement) statement))
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  void should_not_batch_one_statement_when_batching_by_routing_key() {
    StatementBatcher batcher = new DefaultStatementBatcher();
    List<Statement<?>> statements = batcher.batchByGroupingKey(stmt1);
    assertThat(statements).containsOnly(stmt1);
  }

  @Test
  void should_not_batch_one_statement_when_batching_all() {
    StatementBatcher batcher = new DefaultStatementBatcher();
    List<Statement<?>> statements = batcher.batchAll(stmt1);
    assertThat(statements).hasSize(1);
    Statement<?> statement = statements.get(0);
    assertThat(statement).isSameAs(stmt1);
  }

  @Test
  void should_honor_max_statements_in_batch() {
    assignRoutingTokens();
    StatementBatcher batcher = new DefaultStatementBatcher(2);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_honor_max_size_in_bytes() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_high() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(1000);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }

  @Test
  void should_buffer_by_max_size_in_bytes_if_satisfied_before_max_batch_statements() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(10, 8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_buffer_by_max_batch_statements_if_satisfied_before_max_size_in_bytes() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(1, 8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1), tuple(stmt2), tuple(stmt3), tuple(stmt4), tuple(stmt5), tuple(stmt6));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1), tuple(stmt2), tuple(stmt3), tuple(stmt4), tuple(stmt5), tuple(stmt6));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_high() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(100, 1000);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_negative() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new DefaultStatementBatcher(-1, -1);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6));
  }

  protected void assignRoutingKeys() {
    stmt1 = stmt1.setRoutingKey(key1).setRoutingToken(null);
    stmt2 = stmt2.setRoutingKey(key1).setRoutingToken(null);
    stmt3 = stmt3.setRoutingKey(key2).setRoutingToken(null);
    stmt4 = stmt4.setRoutingKey(key2).setRoutingToken(null);
    stmt5 = stmt5.setRoutingKey(key3).setRoutingToken(null);
    stmt6 = stmt6.setRoutingKey(key1).setRoutingToken(null);
  }

  protected void assignRoutingTokens() {
    stmt1 = stmt1.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2 = stmt2.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3 = stmt3.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4 = stmt4.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5 = stmt5.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6 = stmt6.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }

  protected void assignRoutingTokensWitSize() {
    stmt1 = stmt1.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2 = stmt2.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3 = stmt3.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4 = stmt4.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5 = stmt5.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6 = stmt6.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }
}
