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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.DataType;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class MetaInfo {

    static final Logger LOG = LoggerFactory.getLogger(MetaInfo.class);

    final Map<String, String> indices = new HashMap<>();
    final List<String> constants = new ArrayList<>();
    final Map<String, String> variable2Dimension = new HashMap<>();
    final Map<String, Integer> dimensionSize = new HashMap<>();
    final Map<String, Long> variableSize = new HashMap<>();
    final HashMultimap<String, String> variableDimensionCache = HashMultimap.create();
    final Set<String> dimensionCache = new HashSet<>();
    public final NetcdfFile ncfile;

    private MetaInfo(NetcdfFile ncfile) {
        this.ncfile = ncfile;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BoundsInfo(\n");
        sb.append("Variables: ");
        sb.append(variable2Dimension);
        sb.append('\n');
        sb.append("Sizes: ");
        sb.append(dimensionSize);
        sb.append("\n)");
        return sb.toString();
    }

    public boolean isBounds(Variable v) {
        return variable2Dimension.containsKey(v.getFullNameEscaped());
    }

    public boolean isBounds(String varName) {
        return variable2Dimension.containsKey(varName);
    }

    public boolean isBounds(Variable v, Dimension d) {
        String dimS = variable2Dimension.get(v.getFullNameEscaped());
        if (dimS == null) {
            return false;
        } else {
            return dimS.equals(d.getFullNameEscaped());
        }
    }

    public boolean isDescription(Variable v, Dimension d) {
        return v.getFullNameEscaped().equals(d.getFullNameEscaped());
    }

    public boolean isDescription(String varName) {
        return dimensionCache.contains(varName);
    }
    
    public boolean isConstant(String varName) {
        return constants.contains(varName);
    }
    
    public Set<String> getConstants() {
        return new HashSet<>(constants);
    }

    public boolean isIndex(String field) {
        return indices.containsKey(field);
    }

    public boolean canBeBounds(Dimension d) {
        for (String bdim : variable2Dimension.values()) {
            if (bdim.equals(d.getFullNameEscaped())) {
                return true;
            }
        }
        return false;
    }

    public String getBoundsDimension(Variable v) {
        return variable2Dimension.get(v.getFullNameEscaped());
    }

    public String getDimensionName(String fieldName) {
        return indices.get(fieldName);
    }

    public int getDimSize(Variable v) {
        String dimS = variable2Dimension.get(v.getFullNameEscaped());
        return dimensionSize.get(dimS);
    }

    public int getDimIndex(Variable v) {
        String dimS = variable2Dimension.get(v.getFullNameEscaped());
        return v.findDimensionIndex(dimS);
    }

    public long getVarSize(Variable v) {
        return variableSize.get(v.getFullNameEscaped());
    }

    public Variable getVariable(String varName) {
        return ncfile.findVariable(varName);
    }

    public ImmutableSet<String> getDimensions(String vName) {
        return ImmutableSet.copyOf(variableDimensionCache.get(vName));
    }

    public static MetaInfo fromNetCDF(NetcdfFile ncfile) {
        LOG.debug("Global Attrs:");
        LOG.debug(ncfile.getGlobalAttributes().toString());
        LOG.debug("Dimensions:");
        LOG.debug(ncfile.getDimensions().toString());
        LOG.debug("Variables:");
        LOG.debug(ncfile.getVariables().toString());
        
        MetaInfo mInfo = new MetaInfo(ncfile);
        Set<String> normalDims = new HashSet<>();
        Set<String> bndsDims = new HashSet<>();
        for (Variable v
                : ncfile.getVariables()) {
            if (v.getFullNameEscaped().contains("bnds")) {
                for (Dimension d : v.getDimensions()) {
                    bndsDims.add(d.getFullNameEscaped());
                }
            } else {
                for (Dimension d : v.getDimensions()) {
                    normalDims.add(d.getFullNameEscaped());
                }
            }
        }
        Sets.SetView<String> boundDim = Sets.difference(bndsDims, normalDims);

        if (boundDim.isEmpty()) {
            LOG.info("No bounds dimension found. Bounds Variables Dimensions are {} and normal Variable Dimensions are {}", bndsDims, normalDims);
        } else {
            for (String boundDimensionName : boundDim) {
                Dimension d = ncfile.findDimension(boundDimensionName);
                if (d == null) {
                    throw new RuntimeException("Couldn't find dimension for " + boundDimensionName);
                }
                mInfo.dimensionSize.put(boundDimensionName, d.getLength());
            }
            for (Variable v : ncfile.getVariables()) {
                if (v.getFullNameEscaped().contains("bnds")) {
                    for (Dimension d : v.getDimensions()) {
                        if (boundDim.contains(d.getFullNameEscaped())) {
                            mInfo.variable2Dimension.put(v.getFullNameEscaped(), d.getFullNameEscaped());
                        }
                    }
                }
            }
        }
        for (Dimension d
                : ncfile.getDimensions()) {
            mInfo.dimensionCache.add(d.getFullNameEscaped());
            if (!mInfo.canBeBounds(d)) { // also create an index field for this dimension
                String name = d.getFullNameEscaped() + "_index";
                mInfo.indices.put(name, d.getFullNameEscaped());
            }
        }
        for (Variable v
                : ncfile.getVariables()) {
            List<Dimension> dims = v.getDimensions();
            mInfo.variableDimensionCache.putAll(v.getFullNameEscaped(), dims.stream().map(d -> d.getFullNameEscaped())::iterator);
            if (NetCDFUtils.isConstant(dims)) {
                mInfo.constants.add(v.getFullNameEscaped());
                mInfo.variableSize.put(v.getFullNameEscaped(), Long.valueOf(v.getDataType().getSize()));
            } else {
                if (v.getDataType() != DataType.STRING) {
                    long size = v.getSize() * v.getElementSize();
                    mInfo.variableSize.put(v.getFullNameEscaped(), size);
                } else {
                    throw new RuntimeException("String variables aren't supported at the moment as their size cannot be calculated");
                }
            }
        }

        return mInfo;
    }
}
