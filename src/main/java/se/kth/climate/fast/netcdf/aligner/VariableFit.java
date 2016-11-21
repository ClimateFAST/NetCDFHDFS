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
package se.kth.climate.fast.netcdf.aligner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import se.kth.climate.fast.netcdf.DataDescriptor;

/**
 *
 * @author lkroll
 */
public class VariableFit {

    public final ImmutableMap<String, Long> recordsForVar;
    public final long numberOfFiles;
    public final ImmutableList<DataDescriptor> dataDescriptors;

    private VariableFit(ImmutableMap<String, Long> recordsForVar, long numFiles, ImmutableList<DataDescriptor> dds) {
        this.recordsForVar = recordsForVar;
        this.numberOfFiles = numFiles;
        this.dataDescriptors = dds;
    }

    public static VariableFit fromDataDescriptors(ImmutableList<DataDescriptor> dds) {
        DataDescriptor firstDD = dds.get(0); // use first descriptor for records as that one has the largest number of records per variable
        ImmutableMap.Builder<String, Long> rfvb = ImmutableMap.builder();
        for (String varName : firstDD.vars) {
            rfvb.put(varName, firstDD.variableSize(varName));
        }
        return new VariableFit(rfvb.build(), dds.size(), dds);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VFit(records=");
        sb.append(recordsForVar);
        sb.append(", files=");
        sb.append(numberOfFiles);
        sb.append(")");
        return sb.toString();
    }
}
