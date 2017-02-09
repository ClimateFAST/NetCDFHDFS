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
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static se.kth.climate.fast.FASTConstants.META_NAME;
import se.kth.climate.fast.common.Metadata;
import static se.kth.climate.fast.netcdf.NetCDFConstants.SUFFIX;
import se.kth.climate.fast.netcdf.aligner.VariableAlignment;
import se.kth.climate.fast.netcdf.aligner.VariableAssignment;
import se.kth.climate.fast.netcdf.aligner.VariableFit;
import se.kth.climate.fast.netcdf.metadata.GsonSink;
import se.kth.climate.fast.netcdf.metadata.MetaSink;
import se.kth.climate.fast.netcdf.metadata.MetaSinkFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class NetCDFWriter {

    static final Logger LOG = LoggerFactory.getLogger(NetCDFWriter.class);
    
    

    private final File tmpDir;

    {
        tmpDir = Files.createTempDir();
        tmpDir.deleteOnExit();
    }

    public void write(VariableAlignment va, WorkQueue<File> progressPipe) throws IOException {
        for (Pair<VariableAssignment, VariableFit> pvv : va) {
            final VariableAssignment vas = pvv.getValue0();
            final VariableFit vf = pvv.getValue1();
            for (DataDescriptor dd : vf.dataDescriptors) {
                FileInfo fi;
                if (dd.splitDim.isPresent()) {
                    DimensionRange dr = dd.dims.get(dd.splitDim.get());
                    ImmutableList<String> vars = ImmutableList.copyOf(Collections2.filter(dd.vars,
                            Predicates.not(Predicates.equalTo(dd.splitDim.get()))));
                    Variable dimV = dd.metaInfo.getVariable(dd.splitDim.get());
                    if (dimV == null || (dimV.getDimensions().size() != 1)) {
                        fi = new FileInfo(vars, dr);
                    } else { // single dimension variable
                        Array data = dimV.read(); // maybe too big, but should be ok
                        Optional<TypedRange> tr = TypedRange.fromArray(data, Ints.checkedCast(dr.start), Ints.checkedCast(dr.end));
                        if (tr.isPresent()) {
                            fi = new FileInfo(vars, tr.get(), dr);
                        } else {
                            fi = new FileInfo(vars, dr);
                        }
                    }
                } else {
                    fi = new FileInfo(dd.vars.asList());
                }
                String fname = FileNameFormat.serialise(fi) + SUFFIX;
                File f = tmpDir.toPath().resolve(fname).toFile();
                if (f.createNewFile()) {
                    try (NetcdfFileWriter writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, f.getAbsolutePath())) {
                        HashMap<String, Dimension> newDims = new HashMap<>();
                        dd.dims.values().forEach((dr) -> {
                            Dimension d = writer.addDimension(null, dr.name, (int) dr.getSize());
                            d.setUnlimited(dr.inf);
                            newDims.put(dr.name, d);
                        });
                        HashMap<String, Variable> newVars = new HashMap<>();
                        dd.vars.forEach((varName) -> {
                            Variable vOld = dd.metaInfo.getVariable(varName);
                            List<Dimension> vdims = vOld.getDimensions().stream().map(
                                    (d) -> newDims.get(d.getFullName())
                            ).collect(Collectors.toList());
                            Variable vNew = writer.addVariable(null, varName, vOld.getDataType(), vdims);
                            newVars.put(varName, vNew);
                        });
                        writer.create();
                        NetcdfFile origin = dd.metaInfo.ncfile;
                        for (Variable var : newVars.values()) {
                            Variable vOld = dd.metaInfo.getVariable(var.getFullName());
                            List<Range> ranges = vOld.getDimensions().stream().map((d)
                                    -> dd.dims.get(d.getFullName()).toRange()
                            ).collect(Collectors.toList());
                            Array data = vOld.read(ranges);
                            writer.write(var, data);
                        }
                    } catch (InvalidRangeException ex) {
                        LOG.error("Error on reading/writing variable!", ex);
                        throw new IOException(ex);
                    }
                    LOG.info("Wrote file {}.", f.getAbsolutePath());
                    if (progressPipe != null) {
                        progressPipe.put(f);
                    }
                } else {
                    throw new IOException("File already exists: " + f.getAbsolutePath());
                }
            }
        }
    }

    public void writeMeta(Metadata meta, WorkQueue<File> progressPipe) throws IOException {
        File f = tmpDir.toPath().resolve(META_NAME).toFile();
        MetaSinkFactory sinkF = new GsonSink.FileFactory(f);
        try (MetaSink sink = sinkF.create()) {
            sink.sink(meta);

        } catch (Exception ex) {
            throw new IOException(ex);
        }
        LOG.info("Wrote metadata file {}.", f.getAbsolutePath());
        if (progressPipe != null) {
            progressPipe.put(f);
        }
    }

//    private void printFile(File f) throws IOException {
//        System.out.println("Written file of length " + f.length());
//        byte[] bFile = new byte[(int) f.length()];
//
//        try (FileInputStream fis = new FileInputStream(f)) {
//            fis.read(bFile);
//        }
//        String h = Hex.encodeHexString(bFile);
//        System.out.println(h);
//    }
    private String generateFileNamePrefix(VariableAssignment vas) {
        StringBuilder sb = new StringBuilder();
        vas.infVariables.forEach(v -> {
            sb.append(v);
            sb.append("_");
        });
        vas.otherVariables.forEach(v -> {
            sb.append(v);
            sb.append("_");
        });
        return sb.toString();
    }
}
