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
package org.neo4j.gis.spatial.index.curves;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.gis.spatial.index.Envelope;

public abstract class SpaceFillingCurve
{

    /**
     * Description of the space filling curve structure
     */
    abstract static class CurveRule
    {
        final int dimension;
        final int[] npointValues;

        CurveRule( int dimension, int[] npointValues )
        {
            this.dimension = dimension;
            this.npointValues = npointValues;
            assert npointValues.length == length();
        }

        final int length()
        {
            return (int) Math.pow( 2, dimension );
        }

        int npointForIndex( int derivedIndex )
        {
            return npointValues[derivedIndex];
        }

        int indexForNPoint( int npoint )
        {
            for ( int index = 0; index < npointValues.length; index++ )
            {
                if ( npointValues[index] == npoint )
                {
                    return index;
                }
            }
            return -1;
        }

        abstract CurveRule childAt( int npoint );
    }

    private final Envelope range;
    private final int nbrDim;
    private final int maxLevel;
    private final long width;
    private final long valueWidth;
    private final int quadFactor;
    private final long initialNormMask;

    private double[] scalingFactor;

    SpaceFillingCurve( Envelope range, int maxLevel )
    {
        this.range = range;
        this.nbrDim = range.getDimension();
        this.maxLevel = maxLevel;
        if ( maxLevel < 1 )
        {
            throw new IllegalArgumentException( "Hilbert index needs at least one level" );
        }
        if ( range.getDimension() > 3 )
        {
            throw new IllegalArgumentException( "Hilbert index does not yet support more than 3 dimensions" );
        }
        this.width = (long) Math.pow( 2, maxLevel );
        this.scalingFactor = new double[nbrDim];
        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            scalingFactor[dim] = this.width / range.getWidth( dim );
        }
        this.valueWidth = (long) Math.pow( 2, maxLevel * nbrDim );
        this.initialNormMask = (long) (Math.pow( 2, nbrDim ) - 1) << (maxLevel - 1) * nbrDim;
        this.quadFactor = (int) Math.pow( 2, nbrDim );
    }

    public int getMaxLevel()
    {
        return maxLevel;
    }

    public long getWidth()
    {
        return width;
    }

    public long getValueWidth()
    {
        return valueWidth;
    }

    public double getTileWidth( int dimension, int level )
    {
        return range.getWidth( dimension ) / Math.pow( 2, level );
    }

    public Envelope getRange()
    {
        return range;
    }

    protected abstract CurveRule rootCurve();

    /**
     * Given a coordinate in multiple dimensions, calculate its derived key for maxLevel
     */
    public Long derivedValueFor( double[] coord )
    {
        return derivedValueFor( coord, maxLevel );
    }

    /**
     * Given a coordinate in multiple dimensions, calculate its derived key for given level
     */
    private Long derivedValueFor( double[] coord, int level )
    {
        assertValidLevel( level );
        long[] normalizedValues = getNormalizedCoord( coord );
        return derivedValueFor( normalizedValues, level );
    }

    /**
     * Given a normalized coordinate in multiple dimensions, calculate its derived key for maxLevel
     */
    public Long derivedValueFor( long[] normalizedValues )
    {
        return derivedValueFor( normalizedValues, maxLevel );
    }

    /**
     * Given a normalized coordinate in multiple dimensions, calculate its derived key for given level
     */
    private Long derivedValueFor( long[] normalizedValues, int level )
    {
        assertValidLevel( level );
        long derivedValue = 0;
        long mask = 1L << (maxLevel - 1);

        // The starting curve depends on the dimensions
        CurveRule currentCurve = rootCurve();

        for ( int i = 1; i <= maxLevel; i++ )
        {
            int bitIndex = maxLevel - i;
            int npoint = 0;

            for ( long val : normalizedValues )
            {
                npoint = npoint << 1 | (int) ((val & mask) >> bitIndex);
            }

            int derivedIndex = currentCurve.indexForNPoint( npoint );
            derivedValue = (derivedValue << nbrDim) | derivedIndex;
            mask = mask >> 1;
            currentCurve = currentCurve.childAt( derivedIndex );
        }

        if ( level < maxLevel )
        {
            derivedValue = derivedValue << (nbrDim * maxLevel - level);
        }
        return derivedValue;
    }

    /**
     * Given a derived key, find the center coordinate of the corresponding tile at maxLevel
     */
    public double[] centerPointFor( long derivedValue )
    {
        return centerPointFor( derivedValue, maxLevel );
    }

    /**
     * Given a derived key, find the center coordinate of the corresponding tile at given level
     */
    private double[] centerPointFor( long derivedValue, int level )
    {
        long[] normalizedCoord = normalizedCoordinateFor( derivedValue, level );
        return getDoubleCoord( normalizedCoord, level );
    }

    /**
     * Given a derived key, find the normalized coordinate it corresponds to on a specific level
     */
    public long[] normalizedCoordinateFor( long derivedValue )
    {
        return normalizedCoordinateFor( derivedValue, maxLevel );
    }

    /**
     * Given a derived key, find the normalized coordinate it corresponds to on a specific level
     */
    long[] normalizedCoordinateFor( long derivedValue, int level )
    {
        assertValidLevel( level );
        long mask = initialNormMask;
        long[] coordinate = new long[nbrDim];

        // First level is a single curveUp
        CurveRule currentCurve = rootCurve();

        for ( int i = 1; i <= level; i++ )
        {

            int bitIndex = maxLevel - i;

            int derivedIndex = (int) ((derivedValue & mask) >> bitIndex * nbrDim);
            int npoint = currentCurve.npointForIndex( derivedIndex );
            int[] bitValues = bitValues( npoint );

            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                coordinate[dim] = coordinate[dim] << 1 | bitValues[dim];
            }

            mask = mask >> nbrDim;
            currentCurve = currentCurve.childAt( derivedIndex );
        }

        if ( level < maxLevel )
        {
            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                coordinate[dim] = coordinate[dim] << maxLevel - level;
            }
        }

        return coordinate;
    }

    /**
     * Given an envelope, find a collection of LongRange of tiles intersecting it on maxLevel and merge adjacent ones
     */
    public List<LongRange> getTilesIntersectingEnvelope( Envelope referenceEnvelope )
    {
        return getTilesIntersectingEnvelope( referenceEnvelope, new RecursionStats( this.maxLevel ) );
    }

    /**
     * Given an envelope, find a collection of LongRange of tiles intersecting it on maxLevel and merge adjacent ones
     */
    List<LongRange> getTilesIntersectingEnvelope( Envelope referenceEnvelope, RecursionStats stats )
    {
        SearchEnvelope search = new SearchEnvelope( referenceEnvelope );
        ArrayList<LongRange> results = new ArrayList<>(1000);

        addTilesIntersectingEnvelopeAt( stats, 0, search, new SearchEnvelope( 0, this.getWidth(), nbrDim ), rootCurve(), 0, this.getValueWidth(), results );
        return results;
    }

    public static class RecursionStats
    {
        int maxDepth=0;
        int[] counts;
        RecursionStats(int maxLevel)
        {
            this.counts = new int[maxLevel + 1];
        }
        void mark(int depth)
        {
            maxDepth = Math.max( maxDepth, depth );
            counts[depth] ++;
        }
    }

    private void addTilesIntersectingEnvelopeAt( RecursionStats stats, int depth, SearchEnvelope search, SearchEnvelope currentExtent, CurveRule curve, long left, long right,
            ArrayList<LongRange> results )
    {
        if ( right - left == 1 )
        {
            long[] coord = normalizedCoordinateFor( left, maxLevel );
            if ( search.contains( coord ) )
            {
                LongRange current = (results.size() > 0) ? results.get( results.size() - 1 ) : null;
                if ( current != null && current.max == left - 1 )
                {
                    current.expandToMax( left );
                }
                else
                {
                    current = new LongRange( left );
                    results.add( current );
                }
            }
            stats.mark( depth );
        }
        else if ( search.intersects( currentExtent ) )
        {
            double overlap = search.fractionOf( currentExtent );
            if ( overlap >= 0.50 )
            {
                // Note that LongRange upper bound is inclusive, hence the '-1' in several places
                LongRange current = (results.size() > 0) ? results.get( results.size() - 1 ) : null;
                if ( current != null && current.max == left - 1 )
                {
                    current.expandToMax( right - 1 );
                }
                else
                {
                    current = new LongRange( left, right - 1 );
                    results.add( current );
                }
                stats.mark( depth );
            }
            else
            {
                long width = (right - left) / quadFactor;
                for ( int i = 0; i < quadFactor; i++ )
                {
                    int npoint = curve.npointForIndex( i );

                    SearchEnvelope quadrant = currentExtent.quadrant( bitValues( npoint ) );
                    addTilesIntersectingEnvelopeAt( stats, depth + 1, search, quadrant, curve.childAt( i ), left + i * width, left + (i + 1) * width, results );
                }
            }
        }
    }

    /**
     * Bit index describing the in which quadrant an npoint corresponds to
     */
    private int[] bitValues( int npoint )
    {
        int[] bitValues = new int[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            int shift = nbrDim - dim - 1;
            bitValues[dim] = (npoint & (1 << shift)) >> shift;
        }
        return bitValues;
    }

    /**
     * Given a coordinate, find the corresponding normalized coordinate
     */
    private long[] getNormalizedCoord( double[] coord )
    {
        long[] normalizedCoord = new long[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            double value = clamp( coord[dim], range.getMin( dim ), range.getMax( dim ) );
            if ( value == range.getMax( dim ) )
            {
                normalizedCoord[dim] = valueWidth - 1;
            }
            else
            {
                normalizedCoord[dim] = (long) ((value - range.getMin( dim )) * scalingFactor[dim]);
            }
        }
        return normalizedCoord;
    }

    /**
     * Given a normalized coordinate, find the center coordinate of that tile  on the given level
     */
    private double[] getDoubleCoord( long[] normalizedCoord, int level )
    {
        double[] coord = new double[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            double coordinate = ((double) normalizedCoord[dim]) / scalingFactor[dim] + range.getMin( dim ) + getTileWidth( dim, level ) / 2.0;
            coord[dim] = clamp( coordinate, range.getMin( dim ), range.getMax( dim ) );
        }
        return coord;
    }

    private double clamp( double val, double min, double max )
    {
        if ( val <= min )
        {
            return min;
        }
        if ( val >= max )
        {
            return max;
        }
        return val;
    }

    /**
     * Assert that a given level is valid
     */
    private void assertValidLevel( int level )
    {
        if ( level > maxLevel )
        {
            throw new IllegalArgumentException( "Level " + level + " greater than max-level " + maxLevel );
        }
    }

    /**
     * Class for ranges of tiles
     */
    public static class LongRange
    {
        public final long min;
        public long max;

        LongRange( long value )
        {
            this( value, value );
        }

        LongRange( long min, long max )
        {
            this.min = min;
            this.max = max;
        }

        void expandToMax( long other )
        {
            this.max = other;
        }

        @Override
        public boolean equals( Object other )
        {
            return (other instanceof LongRange) && this.equals( (LongRange) other );
        }

        public boolean equals( LongRange other )
        {
            return this.min == other.min && this.max == other.max;
        }

        @Override
        public int hashCode()
        {
            return (int) (this.min << 16 + this.max);
        }

        @Override
        public String toString()
        {
            return "LongRange(" + min + "," + max + ")";
        }
    }

    /**
     * N-dimensional searchEnvelope
     */
    private class SearchEnvelope
    {
        long[] min; // inclusive lower bounds
        long[] max; // exclusive upper bounds
        int nbrDim;

        private SearchEnvelope( Envelope referenceEnvelope )
        {
            this.min = getNormalizedCoord( referenceEnvelope.getMin() );
            this.max = getNormalizedCoord( referenceEnvelope.getMax() );
            this.nbrDim = referenceEnvelope.getDimension();
            for ( int i = 0; i < nbrDim; i++ )
            {
                this.max[i] += 1;
            }
        }

        private SearchEnvelope( long[] min, long[] max )
        {
            this.min = min;
            this.max = max;
            this.nbrDim = min.length;
        }

        private SearchEnvelope( long min, long max, int nbrDim )
        {
            this.nbrDim = nbrDim;
            this.min = new long[nbrDim];
            this.max = new long[nbrDim];

            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                this.min[dim] = min;
                this.max[dim] = max;
            }
        }

        private SearchEnvelope quadrant( int[] quadNbrs )
        {
            long[] newMin = new long[nbrDim];
            long[] newMax = new long[nbrDim];

            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                long extent = (max[dim] - min[dim]) / 2;
                newMin[dim] = this.min[dim] + quadNbrs[dim] * extent;
                newMax[dim] = this.min[dim] + (quadNbrs[dim] + 1) * extent;
            }
            return new SearchEnvelope( newMin, newMax );
        }

        private boolean contains( long[] coord )
        {
            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                if ( coord[dim] < min[dim] || coord[dim] >= max[dim] )
                {
                    return false;
                }
            }
            return true;
        }

        private boolean intersects( SearchEnvelope other )
        {
            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                if ( this.max[dim] <= other.min[dim] || other.max[dim] <= this.min[dim] )
                {
                    return false;
                }
            }
            return true;
        }

        /**
         * The smallest possible envelope has unit area 1
         */
        public double getArea()
        {
            long area = 1;
            for ( int i = 0; i < min.length; i++ )
            {
                area *= max[i] - min[i];
            }
            return area;
        }

        public double fractionOf( SearchEnvelope other )
        {
            SearchEnvelope intersection = this.intersection(other);
            return intersection == null ? 0.0 : intersection.getArea() / other.getArea();
        }

        /**
         * Returns true for the smallest possible envelope, of area 1
         */
        public boolean isUnit()
        {
            for ( int i = 0; i < min.length; i++ )
            {
                if ( max[i] - min[i] > 1 )
                {
                    return false;
                }
            }
            return true;
        }

        public SearchEnvelope intersection( SearchEnvelope other )
        {
            if ( nbrDim == other.nbrDim )
            {
                long[] i_min = new long[this.min.length];
                long[] i_max = new long[this.min.length];
                boolean result = true;
                for ( int i = 0; i < min.length && result; i++ )
                {
                    if ( other.min[i] < this.max[i] && this.min[i] < other.max[i] )
                    {
                        i_min[i] = Math.max(this.min[i], other.min[i]);
                        i_max[i] = Math.min(this.max[i], other.max[i]);
                    }
                    else
                    {
                        result = false;
                    }
                }
                return result ? new SearchEnvelope( i_min, i_max ) : null;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Cannot calculate intersection of Envelopes with different dimensions: " + this.nbrDim + " != " + other.nbrDim );
            }
        }
    }
}
