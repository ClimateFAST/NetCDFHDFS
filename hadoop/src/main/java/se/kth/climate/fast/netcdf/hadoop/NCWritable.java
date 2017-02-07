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
import java.util.UUID;
import org.apache.hadoop.io.Writable;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class NCWritable implements Writable {

    private NetcdfFile ncfile = null;
    private byte[] raw = null;
    
    public NetcdfFile get() {
        return ncfile;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        if (ncfile == null || raw == null) {
            throw new IOException("File is null!");
        }
        out.writeInt(raw.length);
        out.write(raw);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        if (len > 0) {
            raw = new byte[len];
            in.readFully(raw);
            ncfile = NetcdfFile.openInMemory(UUID.randomUUID().toString(), raw); // I think the location needs to be unique           
        } else { 
            throw new IOException("Data for file is empty!");            
        }
    }

    NCWritable(NetcdfFile ncfile, byte[] raw) {
        this.ncfile = ncfile;
        this.raw = raw;
    }
    
    private NCWritable() {}
    
    public static NCWritable fromRaw(byte[] raw, String location) throws IOException {
        NetcdfFile ncfile = NetcdfFile.openInMemory(location, raw);
        return new NCWritable(ncfile, raw);
    }
    
    public static NCWritable read(DataInput in) throws IOException {
        NCWritable w = new NCWritable();
        w.readFields(in);
        return w;
    }

}
