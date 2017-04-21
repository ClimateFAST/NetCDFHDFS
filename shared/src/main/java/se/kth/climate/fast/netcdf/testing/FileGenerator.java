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
package se.kth.climate.fast.netcdf.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.common.DimensionBuilder;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import se.kth.climate.fast.common.VariableBuilder;
import se.kth.climate.fast.netcdf.DimensionRange;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class FileGenerator {

    static final Logger LOG = LoggerFactory.getLogger(FileGenerator.class);
    static final String TITLE = "test";
    static final String DIM = "rows";
    static final String VAR = "values";
    //
    private final int records;

    public FileGenerator(long fileSize) {
        this.records = Ints.checkedCast(fileSize / DataType.INT.getSize());
    }

    public void generate(File f) throws IOException {
        if (!f.canWrite()) {
            throw new IOException("File " + f.getAbsolutePath() + " not writable!");
        }
        try (NetcdfFileWriter writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, f.getAbsolutePath())) {
            Attribute a = writer.addGroupAttribute(null, new Attribute("_title", TITLE));
            Dimension d = writer.addDimension(null, DIM, records);
            //d.setUnlimited(true); // makes writing really slow
            Variable v = writer.addVariable(null, VAR, DataType.INT, ImmutableList.of(d));
            writer.create();
            LOG.debug("Generating data array...");
            Array data = Array.makeArray(DataType.INT, records, 0.0, 1.0);
            LOG.debug("Writing data array...");
            writer.write(v, data);
            LOG.debug("Closing file...");
        } catch (InvalidRangeException ex) {
            throw new IOException(ex);
        }
        LOG.debug("done");
    }

    public Metadata generateMeta() {
        MetadataBuilder mb = new MetadataBuilder();
        mb.addAttribute("_title", TITLE);
        DimensionBuilder db = new DimensionBuilder();
        db.setName(DIM);
        db.setSize(records);
        mb.addDimension(db);
        VariableBuilder vb = new VariableBuilder();
        vb.setStandardName(VAR);
        vb.setShortName(VAR);
        vb.setLongName(VAR);
        vb.setType(DataType.INT);
        vb.addDimension(DIM);
        mb.addVariable(vb);
        return mb.build();
    }

    public boolean checkMeta(Metadata m) {
        boolean correct = true;
        String title = m.getAttribute("_title");
        if ((title == null) || !title.equals(TITLE)) {
            correct = false;
            LOG.warn("Invalid title! Expected: {} but got {}", TITLE, title);
        }
        se.kth.climate.fast.common.Dimension d = m.findDimension(DIM);
        if (d == null) {
            correct = false;
            LOG.warn("Can't find dimension: {}", DIM);
        } else {
            if (d.getSize() != records) {
                correct = false;
                LOG.warn("Reported wrong number of records! Expected: {} but got {}", records, d.getSize());
            }
        }
        se.kth.climate.fast.common.Variable v = m.findVariable(VAR);
        if (v == null) {
            correct = false;
            LOG.warn("Can't find variable: {}", VAR);
        } else {
            if (v.getDataType() != DataType.INT) {
                correct = false;
                LOG.warn("Reported wrong variable type! Expected: {} but got {}", DataType.INT, v.getDataType());
            }
        }
        return correct;
    }

    public List<Path> generateBlocks(File dir, int minBlocks) throws IOException {
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("Directory " + dir.getAbsolutePath() + " is unusable!");
        }
        Path dirP = dir.toPath();
        int recordsPerBlock = records / minBlocks; // might end up with additional blocks to store the fractions
        List<DimensionRange> ranges = new ArrayList<>(minBlocks);
        int offset = 0;
        int lastRec = records - 1;
        while (offset < records) {
            int end = Math.min(lastRec, offset + recordsPerBlock - 1);
            ranges.add(new DimensionRange(DIM, offset, end, true));
            offset += recordsPerBlock;
        }
        LOG.debug("Generated ranges: {}", Iterables.toString(ranges));
        List<Path> paths = new ArrayList<>(ranges.size());
        for (DimensionRange dr : ranges) {
            String name = dr.toName() + ".nc";
            Path fp = dirP.resolve(name).toAbsolutePath();
            LOG.debug("Writing file {}", fp);
            try (NetcdfFileWriter writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fp.toString())) {
                Attribute a = writer.addGroupAttribute(null, new Attribute("_title", TITLE));
                Dimension d = writer.addDimension(null, DIM, (int) dr.getSize());
                //d.setUnlimited(true); // makes writing really slow
                Variable v = writer.addVariable(null, VAR, DataType.INT, ImmutableList.of(d));
                writer.create();
                LOG.debug("Generating data array...");
                Array data = Array.makeArray(DataType.INT, (int) dr.getSize(), dr.start, 1.0);
                LOG.debug("Writing data array...");
                writer.write(v, data);
                LOG.debug("Closing file...");
                paths.add(fp);
            } catch (InvalidRangeException ex) {
                throw new IOException(ex);
            }
            LOG.debug("done");
        }
        return paths;
    }

    public boolean checkBlocks(List<NetcdfFile> ncfiles) throws IOException {
        // files may be out of order so just check the sum of values (very unlikely to match if the files are wrong)
        long sum = 0;
        int count = 0;
        for (NetcdfFile ncfile : ncfiles) {
            LOG.debug("Checking block {}", ncfile.getLocation());
            Triplet<Dimension, Variable, Boolean> dvb = checkHeaders(ncfile, false);
            boolean correct = dvb.getValue2();
            if (!correct) {
                return false; // some required things might be missing
            }
            Dimension d = dvb.getValue0();
            Variable v = dvb.getValue1();
            Array data = v.read();
            for (int i = 0; i < d.getLength(); i++) {
                count++;
                sum += data.getInt(i);
            }
        }
        return checkBlockSum(sum, count);
    }

    public boolean checkBlocksMapped(List<NetcdfFile> ncfiles) throws IOException {
        // files may be out of order so just check the sum of values (very unlikely to match if the files are wrong)
        long sum = 0;
        int count = 0;
        for (NetcdfFile ncfile : ncfiles) {
            LOG.debug("Checking block {}", ncfile.getLocation());
            Triplet<Dimension, Variable, Boolean> dvb = checkHeaders(ncfile, false);
            boolean correct = dvb.getValue2();
            if (!correct) {
                return false; // some required things might be missing
            }
            Dimension d = dvb.getValue0();
            Variable v = dvb.getValue1();
            Array data = v.read();
            for (int i = 0; i < d.getLength(); i++) {
                count++;
                sum += ((int) data.getDouble(i)) / 2;
            }
        }
        return checkBlockSum(sum, count);
    }

    public static Pair<Integer, Long> sumBlock(NetcdfFile ncfile) throws IOException {
        long sum = 0;
        int count = 0;
        Triplet<Dimension, Variable, Boolean> dvb = checkHeaders(ncfile);
        boolean correct = dvb.getValue2();
        if (!correct) {
            return Pair.with(-1, -1l); // some required things might be missing
        }
        Dimension d = dvb.getValue0();
        Variable v = dvb.getValue1();
        Array data = v.read();
        for (int i = 0; i < d.getLength(); i++) {
            count++;
            sum += data.getInt(i);
        }
        return Pair.with(count, sum);
    }

    private static Triplet<Dimension, Variable, Boolean> checkHeaders(NetcdfFile ncfile) {
        boolean correct = true;
        // don't check title as it gets renamed in hadoop
        Dimension d = ncfile.findDimension(DIM);
        if (d == null) {
            correct = false;
            LOG.warn("Can't find dimension: {}", DIM);
        }
        Variable v = ncfile.findVariable(VAR);
        if (v == null) {
            correct = false;
            LOG.warn("Can't find variable: {}", VAR);
        }
        if (!correct) {
            return Triplet.with(null, null, false); // can't continue here
        }
        Dimension vdim = v.getDimensionsAll().get(0);
        if (!vdim.equals(d)) {
            correct = false;
            LOG.warn("Variable is of wrong dimension! {}", vdim.getFullName());
        }
        return Triplet.with(vdim, v, correct);
    }

    private Triplet<Dimension, Variable, Boolean> checkHeaders(NetcdfFile ncfile, boolean completeFile) {
        boolean correct = true;
        if (!(ncfile.getTitle() != null && ncfile.getTitle().equalsIgnoreCase(TITLE))) {
            if (completeFile) {
                correct = false;
            } // if it was split the title was probably moved to metadata, so don't count it as wrong without
            LOG.warn("Invalid title! Expected: {} but got {}", TITLE, ncfile.getTitle());
        }
        Dimension d = ncfile.findDimension(DIM);
        if (d == null) {
            correct = false;
            LOG.warn("Can't find dimension: {}", DIM);
        }
        Variable v = ncfile.findVariable(VAR);
        if (v == null) {
            correct = false;
            LOG.warn("Can't find variable: {}", VAR);
        }
        if (!correct) {
            return Triplet.with(null, null, false); // can't continue here
        }
        if (completeFile) {
            int len = d.getLength();
            if (len != records) {
                correct = false;
                LOG.warn("Invalid number of records. Expected {} but got {}", records, len);
            }
        }
        Dimension vdim = v.getDimensionsAll().get(0);
        if (!vdim.equals(d)) {
            correct = false;
            LOG.warn("Variable is of wrong dimension! {}", vdim.getFullName());
        }
        return Triplet.with(vdim, v, correct);
    }

    public boolean check(NetcdfFile ncfile) throws IOException {
        Triplet<Dimension, Variable, Boolean> dvb = checkHeaders(ncfile, true);
        boolean correct = dvb.getValue2();
        if (!correct) {
            return false; // some required things might be missing
        }
        Dimension d = dvb.getValue0();
        Variable v = dvb.getValue1();
        Array data = v.read();
        for (int i = 0; i < d.getLength(); i++) {
            int val = data.getInt(i);
            if (i != val) {
                correct = false;
                LOG.warn("Invalid value! Expected {} but got {}", i, val);
            }
        }
        return correct;
    }

    public boolean checkBlockSum(long sum, int count) {
        if (count != records) {
            LOG.warn("Counts don't match! Expected {} but got {}", records, count);
            return false;
        }
        long lrec = records;
        long target = (lrec * (lrec - 1)) / 2l; // we start at 0 not at 1
        if (sum != target) {
            LOG.warn("Sums don't match! Expected {} but got {}", target, sum);
            return false;
        }
        return true;
    }
}
