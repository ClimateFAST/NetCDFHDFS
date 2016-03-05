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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import se.kth.climate.fast.common.Constant;
import se.kth.climate.fast.common.DimensionBuilder;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import se.kth.climate.fast.common.VariableBuilder;
import se.kth.climate.fast.netcdfparquet.AvroSchemaGenerator.MetaInfo;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class NCRecordGenerator {

    private final Schema avroSchema;
    private final MetaInfo bInfo;
    private final RecordSink sink;
    private final UserConfig uc;
    private final CrossFileData xfData;
    //
    private List<DataWrapper> varsInAvro;
    private int[] largestShape;

    public NCRecordGenerator(Schema avroSchema, MetaInfo bi, RecordSink sink, UserConfig uc) {
        this.avroSchema = avroSchema;
        this.bInfo = bi;
        this.sink = sink;
        this.uc = uc;
        this.xfData = new CrossFileData(uc.xfAcc);
    }

    public Metadata prepare(NetcdfFile ncfile) throws IOException {
        MetadataBuilder meta = new MetadataBuilder();
        // reset
        varsInAvro = new ArrayList<>();
        largestShape = null;

        DataWrapper largestVar = VariableData.NONE;
        // GLOBAL
        for (Attribute attr : ncfile.getGlobalAttributes()) {
            meta.addAttribute(attr.getFullNameEscaped(), attr.getStringValue());
        }
        // CONSTANTS
        for (String conS : bInfo.constants) {
            VariableBuilder vb = new VariableBuilder();
            Variable v = ncfile.findVariable(conS);
            if (v != null) {
                fillMeta(v, vb);
                Constant c = meta.addConstant(vb);
                Array a = v.read();
                switch (v.getDataType()) {
                    case DOUBLE:
                        c.set(a.getDouble(Index.scalarIndexImmutable));
                        break;
                    case FLOAT:
                        c.set(a.getFloat(Index.scalarIndexImmutable));
                        break;
                    case LONG:
                        c.set(a.getLong(Index.scalarIndexImmutable));
                        break;
                    case INT:
                        c.set(a.getInt(Index.scalarIndexImmutable));
                        break;
                    case BOOLEAN:
                        c.set(a.getBoolean(Index.scalarIndexImmutable));
                        break;
                    default:
                        LOG.warn("No available primitive {} for constant {}!", v.getDataType(), v);

                }
            } else {
                LOG.warn("Could not find variable for field {} in dataset, but is defined as constant!", conS);
            }
        }
        for (Schema.Field f : avroSchema.getFields()) {
            String fieldName = f.name();
            if (bInfo.isIndex(fieldName)) {
                DimensionBuilder db = new DimensionBuilder();
                Dimension d = ncfile.findDimension(bInfo.getDimensionName(fieldName));
                if (d != null) {
                    fillMeta(d, db);
                    DimensionData dd;
                    if (d.isUnlimited()) {
                        dd = xfData.registerDimension(d);
                    } else {
                        dd = new DimensionData(d);
                    }
                    LOG.debug("Adding dimension {} to output set", d.getFullNameEscaped());
                    varsInAvro.add(dd);
                } else {
                    LOG.warn("Could not find dimension for field {} in dataset, but is defined in schema!", fieldName);
                }
                meta.addDimension(db);
            } else {
                VariableBuilder vb = new VariableBuilder();
                Variable v = ncfile.findVariable(fieldName);
                if (v != null) {
                    fillMeta(v, vb);
                    DataWrapper dw;
                    if (xfData.checkVariable(fieldName)) {
                        dw = xfData.registerVariable(v);
                    } else {
                        dw = new VariableData(v, bInfo.getBoundsDimension(v));
                    }
                    LOG.debug("Adding variable {} to output set", v.getFullNameEscaped());
                    varsInAvro.add(dw);
                    if (!bInfo.isBounds(v)) { // don't count bounds vars for the largest var
                        if (dw.getRank() > largestVar.getRank()) {
                            largestVar = dw;
                        }
                    } else {
                        Integer bSize = bInfo.getDimSize(v);
                        vb.addAttribute("bounds_size", bSize.toString());
                    }
                } else {
                    LOG.warn("Could not find variable for field {} in dataset, but is defined in schema!", f.name());
                }
                meta.addVariable(vb);
            }
        }
        findDimensionMaps(largestVar);
        largestShape = largestVar.getShape();
        LOG.debug("Discovered shape of: {}", Arrays.toString(largestShape));
        return meta.build();
    }

    public int generate(NetcdfFile ncfile) throws IOException {
        if ((largestShape == null) || (varsInAvro == null)) {
            throw new RuntimeException("Run prepare() before generate()!");
        }
        int generated = generate(0);
        updateAccums();
        return generated;
    }

    private int generate(int dim) throws IOException {
        int count = 0;
        if (dim < (largestShape.length - 1)) { // recurse
            for (int i = 0; i < largestShape[dim]; i++) {
                updateDims(dim, i);
                count += generate(dim + 1);
            }
            return count;
        } else { // generate
            for (int i = 0; i < largestShape[dim]; i++) {
                updateDims(dim, i);
                GenericData.Record rec = new GenericData.Record(avroSchema);
                for (int var = 0; var < varsInAvro.size(); var++) {
                    DataWrapper dw = varsInAvro.get(var);
                    if (dw.isBounds()) {
                        VariableData vd = (VariableData) dw; // only variables can be bounds
                        int bSize = bInfo.getDimSize(vd.v);
                        int bDimIndex = bInfo.getDimIndex(vd.v);
                        try {
                            Object[] boundsArray = new Object[bSize];
                            for (int bIndex = 0; bIndex < bSize; bIndex++) {
                                vd.index.setDim(bDimIndex, bIndex);
                                boundsArray[bIndex] = vd.getCurrentValue();
                            }
                            rec.put(var, Arrays.asList(boundsArray));
                        } catch (ArrayIndexOutOfBoundsException ex) {
                            LOG.warn("Index was out of bounds for variable {} at {}", vd.v.getFullNameEscaped(), vd.index);
                            rec.put(var, null);
                        }
                    } else {
                        try {
                            Object value = dw.getCurrentValue();
                            rec.put(var, value);
                        } catch (ArrayIndexOutOfBoundsException ex) {
                            LOG.warn("Index was out of bounds for {}", dw);
                            rec.put(var, null);
                        }
                    }
                }
                sink.sink(rec);
                count++;
            }
            return count;
        }
    }

    private void updateDims(int dim, int i) throws IOException {
        for (DataWrapper dw : varsInAvro) {
            dw.updateIndex(dim, i);
        }
    }

    private void findDimensionMaps(DataWrapper largestVar) {
        List<Dimension> lDims = largestVar.getDimensions();
        Map<String, Integer> lDimNames = new HashMap<>();
        for (int i = 0; i < lDims.size(); i++) {
            Dimension d = lDims.get(i);
            lDimNames.put(d.getFullNameEscaped(), i);
        }
        for (DataWrapper dw : varsInAvro) {
            int[] dimMap = new int[largestVar.getRank()];
            Arrays.fill(dimMap, -1);
            List<Dimension> dims = dw.getDimensions();
            for (int i = 0; i < dims.size(); i++) {
                Dimension d = dims.get(i);
                if (dw.isBounds(d)) {
                    continue; // ignore bounds dimensions
                }
                Integer lDim = lDimNames.get(d.getFullNameEscaped());
                if (lDim == null) {
                    LOG.error("Could not find dimension {} in set of largest dimensions: {}", d.getFullNameEscaped(), lDimNames);
                    throw new RuntimeException("Dimension Mapping failed!");
                }
                dimMap[lDim] = i;
            }
            dw.mapDimensions(dimMap);
        }
    }

    private void fillMeta(Variable v, VariableBuilder vb) {
        vb.setShortName(v.getShortName());
        vb.setStandardName(v.getFullNameEscaped());
        vb.setLongName(v.getFullName());
        vb.setUnit(v.getUnitsString());
        vb.setType(v.getDataType());
        for (Attribute attr : v.getAttributes()) {
            if (se.kth.climate.fast.common.Variable.includeAttribute(attr.getFullNameEscaped())) {
                vb.addAttribute(attr.getFullNameEscaped(), attr.getStringValue());
            }
        }
    }

    private void fillMeta(Dimension d, DimensionBuilder db) {
        db.setName(d.getFullNameEscaped());
        db.setUnlimited(d.isUnlimited());
        db.setSize(d.getLength());
        if (d.isVariableLength()) {
            LOG.warn("Dimension {} is declared variable length. There is no support for this feature, yet!", d);
        }
    }

    private void updateAccums() {
        for (DataWrapper dw : varsInAvro) {
            if (dw instanceof DimensionData) {
                DimensionData dd = (DimensionData) dw;
                if (dd.d.isUnlimited()) {
                    xfData.updateDimensionOffset(dd.d.getFullNameEscaped(), dd.getRawValue() + 1);
                }
            } else if (dw instanceof OffsetVariable) {
                OffsetVariable ofs = (OffsetVariable) dw;
                xfData.updateVariable(ofs.v.getFullNameEscaped(), ofs.getRawValue());
            }
        }
    }

    public static interface DataWrapper {

        public void mapDimensions(int[] dimensionMap);

        public void updateIndex(int dim, int val) throws IOException;

        public Object getCurrentValue();

        public int getRank();

        public boolean isBounds();

        public boolean isBounds(Dimension d);

        public List<Dimension> getDimensions();

        public int[] getShape();
    }

    public static final class VariableData implements DataWrapper {

        public static final VariableData NONE = new VariableData();

        public final Variable v;
        public final String boundsDimension;
        private Array data = null;
        public final Index index;
        private int[] dimensionTranslation;

        public VariableData(Variable v, String boundsDimension) {
            this.v = v;
            this.boundsDimension = boundsDimension;
            index = Index.factory(v.getShape());
        }

        private VariableData() {
            v = null;
            boundsDimension = null;
            data = null;
            index = Index.scalarIndexImmutable;
        }

        @Override
        public void mapDimensions(int[] dimensionMap) {
            this.dimensionTranslation = dimensionMap.clone();
        }

        @Override
        public void updateIndex(int dim, int val) throws IOException {
            int dimT = dimensionTranslation[dim];
            if (dimT >= 0) {
                if (data == null) {
                    data = v.read(); // read everything for now...maybe later read less at once
                }
                index.setDim(dimT, val);
            } // else ignore
        }

        @Override
        public Object getCurrentValue() {
            return data.getObject(index);
        }

        @Override
        public int getRank() {
            return index.getRank();
        }

        @Override
        public boolean isBounds() {
            return this.boundsDimension != null;
        }

        @Override
        public boolean isBounds(Dimension d) {
            return d.getFullNameEscaped().equals(this.boundsDimension);
        }

        @Override
        public String toString() {
            return "variable " + v.getFullNameEscaped() + " at " + index.toStringDebug();
        }

        @Override
        public List<Dimension> getDimensions() {
            return v.getDimensions();
        }

        @Override
        public int[] getShape() {
            return index.getShape();
        }

    }

    public static final class DimensionData implements DataWrapper {

        public final Dimension d;
        private long index = 0;
        private long offset = 0;
        private int dimensionTranslation;

        public DimensionData(Dimension d) {
            this.d = d;
        }

        public DimensionData(Dimension d, long offset) {
            this.d = d;
            this.offset = offset;
        }

        @Override
        public void mapDimensions(int[] dimensionMap) {
            for (int i = 0; i < dimensionMap.length; i++) {
                if (dimensionMap[i] >= 0) {
                    this.dimensionTranslation = i;
                }
            }
        }

        @Override
        public void updateIndex(int dim, int val) throws IOException {
            if (dim == this.dimensionTranslation) {
                index = val;
            }
        }

        @Override
        public Object getCurrentValue() {
            return index + offset;
        }

        @Override
        public int getRank() {
            return 1;
        }

        @Override
        public boolean isBounds() {
            return false;
        }

        @Override
        public boolean isBounds(Dimension d) {
            return false;
        }

        @Override
        public String toString() {
            return "dimension " + d.getFullNameEscaped() + " at " + index;
        }

        @Override
        public List<Dimension> getDimensions() {
            return ImmutableList.of(d);
        }

        @Override
        public int[] getShape() {
            return new int[]{d.getLength()};
        }

        private long getRawValue() {
            return this.index;
        }
    }

    public static final class OffsetVariable implements DataWrapper {

        public final Variable v;
        public final Offsetter ofs;
        //public final String boundsDimension;
        private Array data = null;
        public final Index index;
        private int[] dimensionTranslation;

        OffsetVariable(Variable v, Offsetter ofs) {
            this.v = v;
            this.ofs = ofs;
            index = Index.factory(v.getShape());
        }

        @Override
        public void mapDimensions(int[] dimensionMap) {
            this.dimensionTranslation = dimensionMap.clone();
        }

        @Override
        public void updateIndex(int dim, int val) throws IOException {
            int dimT = dimensionTranslation[dim];
            if (dimT >= 0) {
                if (data == null) {
                    data = v.read(); // read everything for now...maybe later read less at once
                }
                index.setDim(dimT, val);
            } // else ignore
        }

        @Override
        public Object getCurrentValue() {
            return ofs.offset(data.getObject(index));
        }

        @Override
        public int getRank() {
            return index.getRank();
        }

        @Override
        public boolean isBounds() {
            //return this.boundsDimension != null;
            return false;
        }

        @Override
        public boolean isBounds(Dimension d) {
            //return d.getFullNameEscaped().equals(this.boundsDimension);
            return false;
        }

        @Override
        public String toString() {
            return "variable " + v.getFullNameEscaped() + " at " + index.toStringDebug() + " with Accumulator";
        }

        @Override
        public List<Dimension> getDimensions() {
            return v.getDimensions();
        }

        @Override
        public int[] getShape() {
            return index.getShape();
        }

        private Object getRawValue() {
            return data.getObject(index);
        }

    }

    private static final class CrossFileData {

        private final HashSet<String> xfAccInput = new HashSet<>();
        private final HashMap<String, Long> xfDimensions = new HashMap<>();
        private final HashMap<String, Offsetter> xfVariables = new HashMap<>();

        private CrossFileData(String[] xfAcc) {
            xfAccInput.addAll(Arrays.asList(xfAcc));
        }

        DimensionData registerDimension(Dimension d) {
            String name = d.getFullNameEscaped();
            xfDimensions.putIfAbsent(name, 0l);
            long v = xfDimensions.get(name);
            return new DimensionData(d, v);
        }

        void updateDimensionOffset(String name, long offset) {
            long curOffset = xfDimensions.get(name);
            xfDimensions.put(name, curOffset + offset);
        }

        boolean checkVariable(String name) {
            return xfAccInput.contains(name);
        }

        OffsetVariable registerVariable(Variable v) {
            String name = v.getFullNameEscaped();
            Offsetter ofs;
            if (xfVariables.containsKey(name)) {
                ofs = xfVariables.get(name);
                if (name.contains("time")) {
                    DateOffsetter dofs = (DateOffsetter) ofs;
                    dofs.update(v);
                }
            } else {
                if (name.contains("time")) {
                    DateOffsetter dofs = new DateOffsetter(v);
                    dofs.update(v);
                    ofs = dofs;
                } else {
                    ofs = Accumulator.forDataType(v.getDataType());
                }
                xfVariables.put(name, ofs);
            }
            return new OffsetVariable(v, ofs);
        }

        void updateVariable(String name, Object value) {
            Offsetter ofs = xfVariables.get(name);
            if (ofs instanceof Accumulator) {
                Accumulator acc = (Accumulator) ofs;
                acc.update(value);
            } else if (ofs instanceof DateOffsetter) {
                // ignore...is updated on register
            } else {
                LOG.warn("Couldn't update Ofsetter {} in {}", ofs, name);
            }
        }

    }
}
