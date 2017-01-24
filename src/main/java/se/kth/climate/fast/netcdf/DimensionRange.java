/*
 * Copyright (C) 2016 KTH Royal Institute of Technology
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.climate.fast.netcdf;

import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

/**
 *
 * @author lkroll
 */
public class DimensionRange {

    public final String name;
    public final long start;
    public final long end;
    public final boolean inf;

    public DimensionRange(String name, long start, long end, boolean inf) {
        this.name = name;
        this.start = start;
        this.end = end;
        this.inf = inf;
    }

    public long getSize() {
        return (end - start)+1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DimensionRange(name=");
        sb.append(name);
        sb.append(',');
        sb.append(start);
        sb.append('-');
        sb.append(end);
        if (inf) {
            sb.append("(currently)");
        }
        sb.append(")");
        return sb.toString();
    }

    public Range toRange() {
        return toRange(1);
    }

    public Range toRange(int step) {
        try {
            return new Range((int) start, (int) end, step);
        } catch (InvalidRangeException ex) {
            throw new RuntimeException(ex);
        }
    }
}
