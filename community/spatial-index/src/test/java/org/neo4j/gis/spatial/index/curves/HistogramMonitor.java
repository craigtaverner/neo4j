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

public class HistogramMonitor implements SpaceFillingCurveMonitor
{
    private int[] counts;
    private long searchArea;
    private long coveredArea = 0;

    HistogramMonitor( int maxLevel )
    {
        this.counts = new int[maxLevel + 1];
    }

    @Override
    public void addRangeAtDepth( int depth )
    {
        this.counts[depth]++;
    }

    @Override
    public void registerSearchArea( long size )
    {
        this.searchArea = size;
    }

    @Override
    public void addToCoveredArea( long size )
    {
        this.coveredArea += size;
    }

    public int[] getCounts()
    {
        return this.counts;
    }

    public long getSearchArea()
    {
        return searchArea;
    }

    public long getCoveredArea()
    {
        return coveredArea;
    }
}
