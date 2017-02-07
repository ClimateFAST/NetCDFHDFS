/*
 * Copyright (C) 2017 KTH Royal Institute of Technology
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
package se.kth.climate.fast.netcdf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class CountSumWritable implements Writable {

    private int count;
    private long sum;

    public int getCount() {
        return count;
    }

    public long getSum() {
        return sum;
    }

    public CountSumWritable() {
    }

    public CountSumWritable(int count, long sum) {
        this.count = count;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(count);
        d.writeLong(sum);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        count = di.readInt();
        sum = di.readLong();
    }

    @Override
    public String toString() {
        return Integer.toString(count) + ":" + Long.toString(sum);
    }

}
