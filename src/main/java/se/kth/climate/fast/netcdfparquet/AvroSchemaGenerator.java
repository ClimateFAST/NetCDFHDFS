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
package se.kth.climate.fast.netcdfparquet;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.math3.util.Pair;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.ma2.DataType;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class AvroSchemaGenerator {

    public static Pair<Schema, MetaInfo> fromNetCDF(NetcdfFile ncfile) {
        SchemaBuilder.RecordBuilder<Schema> rb = SchemaBuilder.record(ncfile.getTitle()).namespace("netcdf");
        SchemaBuilder.FieldAssembler<Schema> fields = rb.fields();
        MetaInfo bi = findBounds(ncfile);
        LOG.info("Using bounds: {}", bi);
        for (Dimension d : ncfile.getDimensions()) {
            if (!bi.canBeBounds(d)) { // also create an index field for this dimension
                String name = d.getFullNameEscaped()+"_index";
                SchemaBuilder.FieldBuilder<Schema> field = fields.name(name);
                if (d.isUnlimited()) {
                    field.type().longType().noDefault();
                } else {
                    field.type().intType().noDefault();
                }
                bi.indices.put(name, d.getFullNameEscaped());
            }
        }
        for (Variable v : ncfile.getVariables()) {
            List<Dimension> dims = v.getDimensions();
            if (dims.isEmpty()) {
                LOG.info("Not including variable {} in record, since it's a constant.", v.getFullNameEscaped());
                bi.constants.add(v.getFullNameEscaped());
                continue;
            }
            SchemaBuilder.FieldBuilder<Schema> field = fields.name(v.getFullNameEscaped());
            matchFieldType(v.getDataType(), field, bi.isBounds(v));

        }
        return new Pair(fields.endRecord(), bi);
    }

    private static void matchFieldType(DataType type, SchemaBuilder.FieldBuilder<Schema> field, boolean inlineArray) {
        switch (type) {
            case BOOLEAN:
                if (inlineArray) {
                    field.type().array().items().booleanType().noDefault();
                } else {
                    field.type().nullable().booleanType().noDefault();
                }
                break;
            case BYTE:
                if (inlineArray) {
                    field.type().array().items().intType().noDefault();
                } else {
                    field.type().nullable().intType().noDefault();
                }
                break;
            case CHAR:
                if (inlineArray) {
                    field.type().array().items().stringType().noDefault();
                } else {
                    field.type().nullable().stringType().noDefault();
                }
                break;
            case SHORT:
                if (inlineArray) {
                    field.type().array().items().intType().noDefault();
                } else {
                    field.type().nullable().intType().noDefault();
                }
                break;
            case INT:
                if (inlineArray) {
                    field.type().array().items().intType().noDefault();
                } else {
                    field.type().nullable().intType().noDefault();
                }
                break;
            case LONG:
                if (inlineArray) {
                    field.type().array().items().longType().noDefault();
                } else {
                    field.type().nullable().longType().noDefault();
                }
                break;
            case FLOAT:
                if (inlineArray) {
                    field.type().array().items().floatType().noDefault();
                } else {
                    field.type().nullable().floatType().noDefault();
                }
                break;
            case DOUBLE:
                if (inlineArray) {
                    field.type().array().items().doubleType().noDefault();
                } else {
                    field.type().nullable().doubleType().noDefault();
                }
                break;
            case STRING:
                if (inlineArray) {
                    field.type().array().items().stringType().noDefault();
                } else {
                    field.type().nullable().stringType().noDefault();
                }
                break;
            default:
                Main.LOG.warn("No avro type for {}", type);
        }
    }

    private static MetaInfo findBounds(NetcdfFile ncfile) {
        MetaInfo bi = new MetaInfo();
        Set<String> normalDims = new HashSet<>();
        Set<String> bndsDims = new HashSet<>();
        for (Variable v : ncfile.getVariables()) {
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
        SetView<String> boundDim = Sets.difference(bndsDims, normalDims);
        if (boundDim.isEmpty()) {
            LOG.info("No bounds dimension found. Bounds Variables Dimensions are {} and normal Variable Dimensions are {}", bndsDims, normalDims);
            return bi;
        }
        for (String boundDimensionName : boundDim) {
            Dimension d = ncfile.findDimension(boundDimensionName);
            if (d == null) {
                throw new RuntimeException("Couldn't find dimension for " + boundDimensionName);
            }
            bi.dimensionSize.put(boundDimensionName, d.getLength());
        }
        for (Variable v : ncfile.getVariables()) {
            if (v.getFullNameEscaped().contains("bnds")) {
                for (Dimension d : v.getDimensions()) {
                    if (boundDim.contains(d.getFullNameEscaped())) {
                        bi.variable2Dimension.putIfAbsent(v.getFullNameEscaped(), d.getFullNameEscaped());
                    }
                }
            }
        }
        return bi;
    }

    public static class MetaInfo {

        final Map<String, String> indices = new HashMap<>();
        final List<String> constants = new ArrayList<>();
        final Map<String, String> variable2Dimension = new HashMap<>();
        final Map<String, Integer> dimensionSize = new HashMap<>();

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

        public boolean isBounds(Variable v, Dimension d) {
            String dimS = variable2Dimension.get(v.getFullNameEscaped());
            if (dimS == null) {
                return false;
            } else {
                return dimS.equals(d.getFullNameEscaped());
            }
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
    }
}
