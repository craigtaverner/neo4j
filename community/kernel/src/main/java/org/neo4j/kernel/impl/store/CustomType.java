/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.CustomValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class CustomType
{
    /**
     * Handler for header information for Custom objects and arrays of Custom objects
     */
    public static class CustomHeader
    {
        public static final int CUSTOM_HEADER_SIZE = 3;
        private final int length;

        private CustomHeader( int length )
        {
            this.length = length;
        }

        private void writeArrayHeaderTo( byte[] bytes )
        {
            bytes[0] = (byte) PropertyType.CUSTOM.intValue();
            writeLength( bytes, length, 2, 1 );
        }

        static CustomType.CustomHeader fromArrayHeaderBytes( byte[] header )
        {
            int length = readLength( header, 2, 0 );
            return new CustomType.CustomHeader( length );
        }

        public static CustomType.CustomHeader fromArrayHeaderByteBuffer( ByteBuffer buffer )
        {
            return fromArrayHeaderBytes( buffer.array() );
        }
    }

    public static int LENGTH_BYTES_PER_OBJECT = 1;
    public static int MAX_OBJECT_SIZE = 1 << (8 * LENGTH_BYTES_PER_OBJECT);

    public static void writeLength( byte[] bytes, int length, int lengthBytes, int offset )
    {
        for ( int l = 0; l < lengthBytes; l++ )
        {
            byte v = (byte) (length & 0xFF << (l * 8));
            bytes[offset + l] = v;
        }
    }

    public static int readLength( byte[] bytes, int lengthBytes, int offset )
    {
        int length = 0;
        for ( int l = 0; l < lengthBytes; l++ )
        {
            length += ((int) bytes[offset + l]) << (l * 8);
        }
        return length;
    }

    public static byte[] encode( CustomValue[] array )
    {
        ArrayList<byte[]> dataList = new ArrayList<>( array.length );
        int totalBytes = 0;
        for ( CustomValue value : array )
        {
            byte[] bytes = value.asPropertyByteArray();
            if ( bytes.length > MAX_OBJECT_SIZE )
            {
                bytes = Arrays.copyOf( bytes, MAX_OBJECT_SIZE );
            }
            totalBytes += bytes.length;
            dataList.add( bytes );
        }
        byte[] data = new byte[totalBytes + LENGTH_BYTES_PER_OBJECT * dataList.size() + CustomHeader.CUSTOM_HEADER_SIZE];
        CustomHeader customHeader = new CustomHeader( dataList.size() );
        customHeader.writeArrayHeaderTo( data );
        for ( int i = 0, index = CustomHeader.CUSTOM_HEADER_SIZE; i < dataList.size(); i++ )
        {
            byte[] bytes = dataList.get( i );
            writeLength( data, bytes.length, LENGTH_BYTES_PER_OBJECT, index );
            index += LENGTH_BYTES_PER_OBJECT;
            System.arraycopy( bytes, 0, data, index, bytes.length );
            index += bytes.length;
        }
        return data;
    }

    public static Value decode( PropertyBlock block )
    {
        return decode( block.getValueBlocks(), 0 );
    }

    private static int customValueLength( long propBlock )
    {
        return (int) ((propBlock & 0x00000000F0000000L) >> 28);
    }

    private static void addBytes( ByteBuffer buffer, long propBlock, int offset, int count )
    {
        byte[] bytes = ByteBuffer.allocate( 8 ).putLong( propBlock ).array();
        for ( int i = 0; i < count; i++ )
        {
            buffer.put( bytes[offset + i] );
        }
    }

    public static Value decode( long[] valueBlocks, int offset )
    {
        long firstBlock = valueBlocks[offset];
        int length = customValueLength( firstBlock );
        ByteBuffer bytes = ByteBuffer.allocate( length );
        // Add the high order bytes from the array
        addBytes( bytes, firstBlock, 4, 4 );
        // Add the rest
        for ( int i = 1; i < valueBlocks.length; i++ )
        {
            long block = valueBlocks[i];
            if ( bytes.position() + 8 < length )
            {
                // Can read all 8 bytes
                bytes.putLong( block );
            }
            else
            {
                // Can read only a few bytes
                addBytes( bytes, block, 0, length - bytes.position() );
            }
        }
        // TODO: Used type injection code to create custom value
        return Values.byteArray( bytes.array() );
    }

    public static Value decodeCustomArray( CustomHeader header, byte[] data )
    {
        // TODO implement
        throw new UnsupportedOperationException( "not implemented" );
    }

}
