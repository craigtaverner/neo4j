/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.storable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.PrettyPrinter;

import static java.lang.String.format;
import static java.util.Arrays.stream;

public class PolygonValue extends ScalarValue implements Comparable<PolygonValue>, CustomValue, Geometry
{
    private CoordinateReferenceSystem crs;
    private Coordinate[] coordinates;

    PolygonValue( CoordinateReferenceSystem crs, Coordinate... coordinates )
    {
        this.crs = crs;
        this.coordinates = coordinates;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeCustomValue( this );
    }

    @Override
    public String prettyPrint()
    {
        PrettyPrinter prettyPrinter = new PrettyPrinter();
        this.writeTo( prettyPrinter );
        return prettyPrinter.value();
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.GEOMETRY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public boolean equals( long x )
    {
        return false;
    }

    @Override
    public boolean equals( double x )
    {
        return false;
    }

    @Override
    public boolean equals( char x )
    {
        return false;
    }

    @Override
    public boolean equals( String x )
    {
        return false;
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof PolygonValue )
        {
            PolygonValue pv = (PolygonValue) other;
            return this.getCoordinateReferenceSystem().equals( pv.getCoordinateReferenceSystem() ) && Arrays.equals( this.coordinates, pv.coordinates );
        }
        return false;
    }

    public boolean equals( Geometry other )
    {
        if ( !other.getCRS().getHref().equals( this.getCRS().getHref() ) )
        {
            return false;
        }
        if ( !other.getGeometryType().equals( this.getGeometryType() ) )
        {
            return false;
        }
        List<Coordinate> otherCoordinates = other.getCoordinates();
        if ( otherCoordinates.size() != this.coordinates.length )
        {
            return false;
        }
        for ( int i = 0; i < this.coordinates.length; i++ )
        {
            if ( otherCoordinates.get( i ) != this.coordinates[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null &&
               (
                       (other instanceof Value && equals( (Value) other )) ||
                       (other instanceof Geometry && equals( (Geometry) other ))
               );
    }

    @Override
    int unsafeCompareTo( Value other )
    {
        return compareTo( (PolygonValue) other );
    }

    public int compareTo( PolygonValue other )
    {
        int cmpCRS = this.crs.getCode() - other.crs.getCode();
        if ( cmpCRS != 0 )
        {
            return cmpCRS;
        }

        if ( this.coordinates.length > other.coordinates.length )
        {
            return 1;
        }
        else if ( this.coordinates.length < other.coordinates.length )
        {
            return -1;
        }

        for ( int i = 0; i < coordinates.length; i++ )
        {
            int dimension = coordinates[i].getCoordinate().size();
            for ( int d = 0; d < dimension; d++ )
            {
                int cmpVal = (int) (this.coordinates[i].getCoordinate().get( d ) - other.coordinates[i].getCoordinate().get( d ));
                if ( cmpVal != 0 )
                {
                    return cmpVal;
                }
            }
        }
        return 0;
    }

    @Override
    Integer unsafeTernaryCompareTo( Value otherValue )
    {
        PolygonValue other = (PolygonValue) otherValue;

        if ( this.crs.getCode() != other.crs.getCode() || this.coordinates.length != other.coordinates.length )
        {
            return null;
        }

        return this.envelope().ternaryCompareTo( other.envelope() );
    }

    Envelope envelope()
    {
        Envelope env = new Envelope();
        for ( Coordinate coordinate : coordinates )
        {
            // TODO: Seriously think of getting rid of List<Double> for Coordinate class
            List<Double> coord = coordinate.getCoordinate();
            double[] coords = new double[coord.size()];
            for ( int i = 0; i < coords.length; i++ )
            {
                coords[i] = coord.get( i );
            }
            env.expandToInclude( coords );
        }
        return env;
    }

    // TODO reverse dependency and use Envelope in spatial-index
    private static class Envelope
    {
        double[] min;
        double[] max;

        Envelope( double[] min, double[] max )
        {
            this.min = min;
            this.max = max;
        }

        Envelope()
        {
            this.min = null;
            this.max = null;
        }

        void expandToInclude( double[] point )
        {
            if ( this.min == null || this.max == null )
            {
                this.min = Arrays.copyOf( point, point.length );
                this.max = Arrays.copyOf( point, point.length );
            }
            else
            {
                int dim = Math.min( point.length, min.length );
                for ( int i = 0; i < dim; i++ )
                {
                    if ( point[i] < min[i] )
                    {
                        min[i] = point[i];
                    }
                    if ( point[i] > max[i] )
                    {
                        max[i] = point[i];
                    }
                }
            }
        }

        Integer ternaryCompareTo( Envelope other )
        {
            if ( allLessThanOrEqual( this.max, other.min ) )
            {
                // This max is less than other min, so entire envelope is less than other
                return -1;
            }
            if ( allLessThanOrEqual( other.max, this.min ) )
            {
                // Other max is less than this min, so entire envelope is greater than other
                return 1;
            }
            return null;
        }

        private static boolean allLessThanOrEqual( double[] a, double[] b )
        {
            if ( a.length != b.length )
            {
                return false;
            }
            for ( int i = 0; i < a.length; i++ )
            {
                if ( a[i] > b[i] )
                {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public PolygonValue asObjectCopy()
    {
        return this;
    }

    public CoordinateReferenceSystem getCoordinateReferenceSystem()
    {
        return crs;
    }

    /*
     * Consumers must not modify the returned array.
     */
    public Coordinate[] coordinates()
    {
        return this.coordinates;
    }

    @Override
    public int computeHash()
    {
        int result = 1;
        result = 31 * result + NumberValues.hash( crs.getCode() );
        for ( Coordinate coordinate : coordinates )
        {
            for ( double v : coordinate.getCoordinate() )
            {
                result = 31 * result + NumberValues.hash( v );
            }
        }
        return result;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapCustomValue( this );
    }

    @Override
    public String toString()
    {
        return format( "Polygon{ %s, %s}", getCoordinateReferenceSystem().getName(), asWKT() );
    }

    @Override
    public String getGeometryType()
    {
        return "Polygon";
    }

    @Override
    public List<Coordinate> getCoordinates()
    {
        return stream( coordinates ).collect( Collectors.toList() );
    }

    @Override
    public CRS getCRS()
    {
        return crs;
    }

    @Override
    public int type()
    {
        // Figure out a way for type ID registration to prevent type conflicts
        return 1001;
    }

    @Override
    public String asString()
    {
        return asWKT();
    }

    public String asWKT()
    {
        StringBuilder sb = new StringBuilder();
        for ( Coordinate coordinate : coordinates )
        {
            if ( sb.length() == 0 )
            {
                sb.append( "POLYGON((" );
            }
            else
            {
                sb.append( "," );
            }
            for ( double coord : coordinate.getCoordinate() )
            {
                sb.append( " " ).append( coord );
            }
        }
        sb.append( "))" );
        return sb.toString();
    }

    @Override
    public byte[] asPropertyByteArray()
    {
        byte[] wkt = asWKT().getBytes( StandardCharsets.UTF_8 );
        return ByteBuffer.allocate( wkt.length + 4 ).putInt( wkt.length ).put( wkt ).array();
    }
}
