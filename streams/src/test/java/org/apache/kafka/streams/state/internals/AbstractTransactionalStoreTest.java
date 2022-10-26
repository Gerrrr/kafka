/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractTransactionalStoreTest<T extends KeyValueStore<Bytes, byte[]>> {
    InternalMockProcessorContext<Object, Object> context;
    AbstractTransactionalStore<T> txnStore;
    final Bytes key1 = Bytes.wrap("key1".getBytes());
    final byte[] val1 = "val1".getBytes();
    final Bytes key2 = Bytes.wrap("key2".getBytes());
    final byte[] val2 = "val2".getBytes();

    @Before
    public void setUp() {
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
        try {
            txnStore = getTxnStore();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        txnStore.close();
    }

    @Test
    public void testGetUncommitted() {
        assertNull(txnStore.get(key1));
        txnStore.put(key1, val1);
        assertArrayEquals(val1, txnStore.get(key1));
    }

    @Test
    public void testGetCommitted() {
        assertNull(txnStore.get(key1));
        txnStore.put(key1, val1);
        txnStore.commit(1L);
        assertArrayEquals(val1, txnStore.get(key1));
    }

    @Test
    public void testGetUncommittedShadowsCommitted() {
        assertNull(txnStore.get(key1));
        txnStore.put(key1, val1);
        txnStore.commit(1L);
        txnStore.put(key1, val2);
        assertArrayEquals(val2, txnStore.get(key1));
    }

    @Test
    public void testPutIfAbsent() {
        assertNull(txnStore.putIfAbsent(key1, val1));
        assertArrayEquals(val1, txnStore.get(key1));

        assertArrayEquals(val1, txnStore.putIfAbsent(key1, val2));
        assertArrayEquals(val1, txnStore.get(key1));

        txnStore.commit(1L);
        assertArrayEquals(val1, txnStore.putIfAbsent(key1, val2));
        assertArrayEquals(val1, txnStore.get(key1));
    }

    @Test
    public void testAllUncommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val1));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.all());
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testAllCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val1));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.all());

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testAllUncommittedShadowsCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val2));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.all());
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseAllUncommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val1));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseAll());
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseAllCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val1));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseAll());

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseAllUncommittedShadowsCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseAll());
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testRangeUncommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val1));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.range(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testRangeCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val1));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.range(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testRangeDisjointUncommittedAndCommitted() {
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val1);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val1));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.range(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testRangeUncommittedShadowsCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, val2));
        expected.add(new KeyValue<>(key2, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.range(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseRangeUncommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val1));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseRange(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseRangeCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val1));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseRange(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseRangeDisjointUncommittedAndCommitted() {
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val1);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val1));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseRange(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testReverseRangeUncommittedShadowsCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.commit(1L);
        txnStore.put(key1, val2);

        final List<KeyValue<Bytes, byte[]>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key2, val2));
        expected.add(new KeyValue<>(key1, val2));
        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore.reverseRange(key1, key2));
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).key, actual.get(i).key);
            assertArrayEquals(expected.get(i).value, actual.get(i).value);
        }
    }

    @Test
    public void testRecovery() {
        txnStore.put(key1, val1);
        txnStore.put(key2, val2);
        txnStore.close();

        final AbstractTransactionalStore<T> txnStore2 = getTxnStore();
        final boolean recovered = txnStore2.recover(1L);
        assertTrue(recovered);

        final List<KeyValue<Bytes, byte[]>> actual = Utils.toList(txnStore2.all());
        assertEquals(0, actual.size());

        txnStore2.close();
    }

    @Test
    public void testDeleteUncommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key1, null);
        assertNull(txnStore.get(key1));
    }

    @Test
    public void testDeleteCommitted() {
        txnStore.put(key1, val1);
        txnStore.put(key1, null);
        txnStore.commit(1L);
        assertNull(txnStore.get(key1));
    }

    @Test
    public void testUncommittedDeleteShadowsCommittedKey() {
        txnStore.put(key1, val1);
        txnStore.commit(1L);
        txnStore.put(key1, null);
        assertNull(txnStore.get(key1));
    }

    @Test
    public void testAllUncommittedDeletionShadowsCommittedKey() {
        txnStore.put(key1, val1);
        txnStore.commit(1L);
        txnStore.put(key1, null);
        txnStore.put(key2, null);

        assertEquals(0, Utils.toList(txnStore.all()).size());
    }

    @Test
    public void testStateStoreCloseClosesOpenIterators() {
        txnStore.put(key1, val1);
        txnStore.commit(1L);
        txnStore.put(key2, val2);

        final KeyValueIterator<Bytes, byte[]> all = txnStore.all();
        assertTrue(all.hasNext());
        txnStore.close();
        assertThrows(
            "RocksDB iterator for store main.tmp has close",
            InvalidStateStoreException.class,
            all::hasNext
        );
    }

    @Test
    public void testPrefixScanUncommittedOnly() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(Bytes.wrap("k1".getBytes()), "a".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_3".getBytes()), "b".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("k2".getBytes()), "c".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_2".getBytes()), "d".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("k3".getBytes()), "e".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_1".getBytes()), "f".getBytes()));
        txnStore.putAll(entries);

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = txnStore.prefixScan("prefix", new StringSerializer())) {
            final List<String> valuesWithPrefix = new ArrayList<>();
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
            assertThat(numberOfKeysReturned, is(3));
            assertThat(valuesWithPrefix.get(0), is("f"));
            assertThat(valuesWithPrefix.get(1), is("d"));
            assertThat(valuesWithPrefix.get(2), is("b"));
        }
    }

    @Test
    public void testPrefixScanDisjointCommittedAndUncommitted() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(Bytes.wrap("k1".getBytes()), "a".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_3".getBytes()), "b".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("k2".getBytes()), "c".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_2".getBytes()), "d".getBytes()));
        txnStore.putAll(entries);
        txnStore.commit(null);
        txnStore.put(Bytes.wrap("k3".getBytes()), "e".getBytes());
        txnStore.put(Bytes.wrap("prefix_1".getBytes()), "f".getBytes());

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = txnStore.prefixScan("prefix", new StringSerializer())) {
            final List<String> valuesWithPrefix = new ArrayList<>();
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
            assertThat(numberOfKeysReturned, is(3));
            assertThat(valuesWithPrefix.get(0), is("f"));
            assertThat(valuesWithPrefix.get(1), is("d"));
            assertThat(valuesWithPrefix.get(2), is("b"));
        }
    }

    @Test
    public void testPrefixScanCommittedShadowsUncommitted() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(Bytes.wrap("k1".getBytes()), "a".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_3".getBytes()), "b".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("k2".getBytes()), "c".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_2".getBytes()), "d".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("k3".getBytes()), "e".getBytes()));
        entries.add(new KeyValue<>(Bytes.wrap("prefix_1".getBytes()), "f".getBytes()));
        txnStore.putAll(entries);
        txnStore.commit(null);
        txnStore.put(Bytes.wrap("prefix_2".getBytes()), "d1".getBytes());
        txnStore.delete(Bytes.wrap("prefix_1".getBytes()));

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = txnStore.prefixScan("prefix", new StringSerializer())) {
            final List<String> valuesWithPrefix = new ArrayList<>();
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
            assertThat(numberOfKeysReturned, is(2));
            assertThat(valuesWithPrefix.get(0), is("d1"));
            assertThat(valuesWithPrefix.get(1), is("b"));
        }
    }

    abstract AbstractTransactionalStore<T> getTxnStore();
}