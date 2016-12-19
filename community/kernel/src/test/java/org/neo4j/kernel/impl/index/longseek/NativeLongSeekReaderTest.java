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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.gbptree.MutableHit;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;

public class NativeLongSeekReaderTest
{
    private static final int LABEL_ID = 1;

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFindMultipleNodesInEachRange() throws Exception
    {
        // GIVEN
        Index<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> index = mock( Index.class );
        RawCursor<Hit<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue>,IOException> cursor = mock( RawCursor.class );
        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.get() ).thenReturn(
                // range, bits
                hit( 0, 0b1000_1000__1100_0010L ),
                hit( 1, 0b0000_0010__0000_1000L ),
                hit( 3, 0b0010_0000__1010_0001L ),
                null );
        when( index.seek( any( LongSeekKey.class ), any( LongSeekKey.class ) ) )
                .thenReturn( cursor );
        try ( org.neo4j.kernel.impl.index.labelscan.NativeLongSeekReader reader = new org.neo4j.kernel.impl.index.labelscan.NativeLongSeekReader( index, 16 ) )
        {
            // WHEN
            PrimitiveLongIterator iterator = reader.nodesWithLabel( LABEL_ID );

            // THEN
            assertArrayEquals( new long[] {
                    1, 6, 7, 11, 15,
                    19, 25,
                    48, 53, 55, 61 },

                    asArray( iterator ) );
        }
    }

    private Hit<LongSeekKey,org.neo4j.kernel.impl.index.labelscan.LongSeekValue> hit( long baseNodeId, long bits )
    {
        LongSeekKey key = new LongSeekKey().set( LABEL_ID, baseNodeId );
        org.neo4j.kernel.impl.index.labelscan.LongSeekValue
                value = new org.neo4j.kernel.impl.index.labelscan.LongSeekValue();
        value.bits = bits;
        return new MutableHit<>( key, value );
    }
}
