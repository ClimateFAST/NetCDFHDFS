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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
import ucar.nc2.Attribute;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class DataDescriptor {

    public final MetaInfo metaInfo;
    public final ImmutableSet<String> vars;
    public final ImmutableMap<String, DimensionRange> dims;
    public final Optional<String> splitDim;

    public DataDescriptor(MetaInfo mi, Set<String> vars, Map<String, DimensionRange> dims, Optional<String> splitDim) {
        this.metaInfo = mi;
        this.vars = ImmutableSet.copyOf(vars);
        this.dims = ImmutableMap.copyOf(dims);
        this.splitDim = splitDim;
    }

    public long estimateSize() {
        return estimateHeaderSize() + estimateDataSize();
    }

    public long estimateHeaderSize() {
        long hs = 0;
        hs += NetCDFConstants.MAGIC_SIZE;
        hs += NetCDFConstants.N_SIZE; // nrecs
        //dim_list   
        hs += NetCDFConstants.TAG_SIZE; // ABSENT | NC_DIMENSION
        if (!dims.isEmpty()) {
            hs += NetCDFConstants.N_SIZE; // nelems
            for (DimensionRange dr : dims.values()) {
                // name
                hs += NetCDFConstants.N_SIZE; // nelems
                hs += dr.name.length(); // namestring
                // dim_length
                hs += NetCDFConstants.N_SIZE;
            }
        }
        //gatt_list 
        hs += NetCDFConstants.TAG_SIZE; // ABSENT | NC_ATTRIBUTE
        // leave empty for now under the assumption that global attributes will be extracted        
        //var_list
        hs += NetCDFConstants.TAG_SIZE; // ABSENT | NC_VARIABLE
        if (!vars.isEmpty()) {
            hs += NetCDFConstants.N_SIZE; // nelems
            for (String varName : vars) {
                Variable v = metaInfo.getVariable(varName);
                // name
                hs += NetCDFConstants.N_SIZE; // nelems
                hs += varName.length(); // namestring
                // dimensions
                hs += NetCDFConstants.N_SIZE; // nelems
                hs += NetCDFConstants.N_SIZE * v.getRank(); // [dimid ...]
                // vatt_list
                hs += NetCDFConstants.TAG_SIZE; // ABSENT | NC_ATTRIBUTE
                hs += NetCDFConstants.N_SIZE; // nelems
                for (Attribute a : v.getAttributes()) {
                    // name 
                    hs += NetCDFConstants.N_SIZE; // nelems
                    hs += a.getFullName().length(); // namestring
                    // type
                    hs += NetCDFConstants.DTYPE_SIZE;
                    hs += NetCDFConstants.N_SIZE; // nelems
                    if (a.getDataType() != DataType.STRING) {
                        hs += a.getDataType().getSize() * a.getLength();
                    } else {
                        Array values = a.getValues();
                        IndexIterator it = values.getIndexIterator();
                        while (it.hasNext()) {
                            String sv = (String) it.getObjectNext();
                            if (sv != null) {
                                hs += sv.length();
                            } else {
                                System.out.println("An object in array was null: " + values);
                            }
                        }
                    }
                }
                // type
                hs += NetCDFConstants.DTYPE_SIZE;
                // vsize
                hs += NetCDFConstants.N_SIZE;
                // begin
                hs += NetCDFConstants.OFFSET_SIZE;
            }
        }
        return hs;
    }

    public long estimateDataSize() {
        long ds = 0;
        for (String vName : vars) {
            Variable v = metaInfo.getVariable(vName);
            ds += NetCDFConstants.ALIGN_SIZE; // assume worst case alignment
            if (v.getDataType() != DataType.STRING) {
                ds += variableSize(vName) * ((long) Math.max(metaInfo.getVarElementSize(v), NetCDFConstants.PADDING_SIZE));
            } else {
                throw new RuntimeException("Can't deal with string variables, yet.");
            }
        }
        return ds;
    }

    public long variableSize(String vName) {
        Set<String> dimensions = metaInfo.getDimensions(vName);
        long s = 1;
        for (String dName : dimensions) {
            DimensionRange dr = dims.get(dName);
            s *= dr.getSize();
        }
        return s;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DataDescriptor(file=");
        sb.append(metaInfo.ncfile.getLocation());
        sb.append(",\n vars=");
        sb.append(vars);
        sb.append(",\n dims=");
        sb.append(dims);
        sb.append("\n)");
        return sb.toString();
    }
}
