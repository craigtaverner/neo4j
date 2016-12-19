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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
//import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Note that returned iterators is backed by a cursor that will be closed when asking for a new iterator from reader
 * or when closing reader.
 */
class NativeLongSeekReader implements LabelScanReader
{
    private final Index<LongSeekKey,LongSeekValue> index;
    private final int rangeSize;
    private Queue<RawCursor<Hit<LongSeekKey,LongSeekValue>,IOException>> openCursors;

    NativeLongSeekReader( Index<LongSeekKey,LongSeekValue> index, int rangeSize )
    {
        this.index = index;
        this.rangeSize = rangeSize;
        this.openCursors = new LinkedList<>();
    }

    @Override
    public void close()
    {
        try
        {
            ensureOpenCursorsClosed();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( int labelId )
    {
        RawCursor<Hit<LongSeekKey,LongSeekValue>,IOException> cursor;
        try
        {
            ensureOpenCursorsClosed();
            cursor = seekerForLabel( labelId );
            openCursors.offer( cursor );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new LongSeekValueIterator( rangeSize, cursor );
    }

    @Override
    public PrimitiveLongIterator nodesWithAnyOfLabels( int... labelIds )
    {
        List<PrimitiveLongIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLongSeekValueIterator( iterators, false );
    }

    @Override
    public PrimitiveLongIterator nodesWithAllLabels( int... labelIds )
    {
        List<PrimitiveLongIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLongSeekValueIterator( iterators, true );
    }

    private List<PrimitiveLongIterator> iteratorsForLabels( int[] labelIds )
    {
        List<PrimitiveLongIterator> iterators = new ArrayList<>();
        try
        {
            ensureOpenCursorsClosed();
            for ( int labelId : labelIds )
            {
                RawCursor<Hit<LongSeekKey,LongSeekValue>,IOException> cursor = seekerForLabel( labelId );
                openCursors.offer( cursor );
                iterators.add( new LongSeekValueIterator( rangeSize, cursor ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return iterators;
    }

    private RawCursor<Hit<LongSeekKey,LongSeekValue>,IOException> seekerForLabel( int labelId ) throws IOException
    {
        LongSeekKey from = new LongSeekKey().set( labelId, 0 );
        LongSeekKey to = new LongSeekKey().set( labelId, Long.MAX_VALUE );
        return index.seek( from, to );
    }

    private void ensureOpenCursorsClosed() throws IOException
    {
        RawCursor<Hit<LongSeekKey,LongSeekValue>,IOException> cursor;
        while ( ( cursor = openCursors.poll() ) != null )
        {
            cursor.close();
        }
    }

    @Override
    public PrimitiveLongIterator labelsForNode( long nodeId )
    {
        throw new UnsupportedOperationException( "Use your db..." );
    }
}
