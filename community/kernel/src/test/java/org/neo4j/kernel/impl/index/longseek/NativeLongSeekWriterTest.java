/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.longseek;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.IndexWriter;
import org.neo4j.index.ValueMerger;
import org.neo4j.index.ValueMergers;
import org.neo4j.index.gbptree.MutableHit;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import static java.lang.Integer.max;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.impl.index.labelscan.NativeLongSeekStoreTest.flipRandom;
import static org.neo4j.kernel.impl.index.labelscan.NativeLongSeekStoreTest.getLabels;
import static org.neo4j.kernel.impl.index.labelscan.NativeLongSeekStoreTest.nodesWithLabel;

public class NativeLongSeekWriterTest
{
    private static final int LABEL_COUNT = 5;
    private static final int RANGE_SIZE = 16;
    private static final int NODE_COUNT = 10_000;
    private static final Comparator<LongSeekKey> KEY_COMPARATOR = new org.neo4j.kernel.impl.index.labelscan.LongSeekLayout( RANGE_SIZE );
    private static final Comparator<Map.Entry<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>> COMPARATOR =
            (o1,o2) -> KEY_COMPARATOR.compare( o1.getKey(), o2.getKey() );

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldAddLabels() throws Exception
    {
        // GIVEN
        ControlledInserter inserter = new ControlledInserter();
        long[] expected = new long[NODE_COUNT];
        try ( org.neo4j.kernel.impl.index.labelscan.NativeLongSeekWriter writer = new org.neo4j.kernel.impl.index.labelscan.NativeLongSeekWriter( inserter, RANGE_SIZE, max( 5, NODE_COUNT / 100 ) ) )
        {
            // WHEN
            for ( int i = 0; i < NODE_COUNT * 3; i++ )
            {
                NodeLabelUpdate update = randomUpdate( expected );
                writer.write( update );
            }
        }

        // THEN
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] expectedNodeIds = nodesWithLabel( expected, i );
            long[] actualNodeIds = asArray( new org.neo4j.kernel.impl.index.labelscan.LongSeekValueIterator( RANGE_SIZE, inserter.nodesFor( i ) ) );
            assertArrayEquals( "For label " + i, expectedNodeIds, actualNodeIds );
        }
    }

    @Test
    public void shouldNotAcceptUnsortedLabels() throws Exception
    {
        // GIVEN
        ControlledInserter inserter = new ControlledInserter();
        boolean failed = false;
        try ( org.neo4j.kernel.impl.index.labelscan.NativeLongSeekWriter writer = new org.neo4j.kernel.impl.index.labelscan.NativeLongSeekWriter( inserter, RANGE_SIZE, 1 ) )
        {
            // WHEN
            writer.write( NodeLabelUpdate.labelChanges( 0, EMPTY_LONG_ARRAY, new long[] {2, 1} ) );
            // we can't do the usual "fail( blabla )" here since the actual write will happen
            // when closing this writer, i.e. in the curly bracket below.
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertTrue( e.getMessage().contains( "unsorted" ) );
            failed = true;
        }

        assertTrue( failed );
    }

    private NodeLabelUpdate randomUpdate( long[] expected )
    {
        int nodeId = random.nextInt( expected.length );
        long labels = expected[nodeId];
        long[] before = getLabels( labels );
        int changeCount = random.nextInt( 4 ) + 1;
        for ( int i = 0; i < changeCount; i++ )
        {
            labels = flipRandom( labels, LABEL_COUNT, random.random() );
        }
        expected[nodeId] = labels;
        return NodeLabelUpdate.labelChanges( nodeId, before, getLabels( labels ) );
    }

    private static class ControlledInserter implements IndexWriter<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>
    {
        private final Map<Integer,Map<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>> data = new HashMap<>();

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public void put( LongSeekKey key, org.neo4j.kernel.impl.index.labelscan.LongSeekValue value ) throws IOException
        {
            merge( key, value, ValueMergers.overwrite() );
        }

        @Override
        public void merge( LongSeekKey key, org.neo4j.kernel.impl.index.labelscan.LongSeekValue value, ValueMerger<org.neo4j.kernel.impl.index.labelscan.LongSeekValue> amender )
                throws IOException
        {
            // Clone since these instances are reused between calls, internally in the writer
            key = clone( key );
            value = clone( value );

            Map<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> forLabel = data.get( key.labelId );
            if ( forLabel == null )
            {
                data.put( key.labelId, forLabel = new HashMap<>() );
            }
            org.neo4j.kernel.impl.index.labelscan.LongSeekValue existing = forLabel.get( key );
            if ( existing == null )
            {
                forLabel.put( key, value );
            }
            else
            {
                amender.merge( existing, value );
            }
        }

        private org.neo4j.kernel.impl.index.labelscan.LongSeekValue clone( org.neo4j.kernel.impl.index.labelscan.LongSeekValue value )
        {
            org.neo4j.kernel.impl.index.labelscan.LongSeekValue
                    result = new org.neo4j.kernel.impl.index.labelscan.LongSeekValue();
            result.bits = value.bits;
            return result;
        }

        private LongSeekKey clone( LongSeekKey key )
        {
            return new LongSeekKey().set( key.labelId, key.idRange );
        }

        @Override
        public org.neo4j.kernel.impl.index.labelscan.LongSeekValue remove( LongSeekKey key ) throws IOException
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }

        @SuppressWarnings( "unchecked" )
        RawCursor<Hit<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>,IOException> nodesFor( int labelId )
        {
            Map<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> forLabel = data.get( labelId );
            if ( forLabel == null )
            {
                forLabel = Collections.emptyMap();
            }

            Map.Entry<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>[] entries =
                    forLabel.entrySet().toArray( new Map.Entry[forLabel.size()] );
            Arrays.sort( entries, COMPARATOR );
            return new RawCursor<Hit<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>,IOException>()
            {
                private int arrayIndex = -1;

                @Override
                public Hit<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> get()
                {
                    Entry<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> entry = entries[arrayIndex];
                    return new MutableHit<>( entry.getKey(), entry.getValue() );
                }

                @Override
                public boolean next()
                {
                    arrayIndex++;
                    return arrayIndex < entries.length;
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
