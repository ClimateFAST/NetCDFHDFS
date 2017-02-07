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

import com.google.common.base.Optional;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import se.kth.climate.fast.netcdf.MetaInfo;
import se.kth.climate.fast.netcdf.NetCDFWriter;
import se.kth.climate.fast.netcdf.WorkQueue;
import se.kth.climate.fast.netcdf.testing.FileGenerator;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author lkroll
 */
public class AlignerTest {

    private static final long MB = 1024 * 1024;
    public static final long BLOCK_SIZE = 64l * MB;

    private WorkQueue<File> q;
    private FileGenerator fg;
    private final File dir;

    {
        dir = Files.createTempDir();
        dir.deleteOnExit();
        System.out.println("Generating instance of test" + dir.getAbsolutePath());
    }

    @Before
    public void setUp() {
        q = new WorkQueue<File>(1000);
        fg = new FileGenerator(200l * MB);
    }

    public AlignerTest() {
    }

    // These tests won't work anywhere but on my laptop, so the last test needs to cover them as well
//    @Test
//    public void testSmall() {
//        try {
//            fg.generate(null);
//            NetcdfFile ncfile = NetcdfFile.open("/Users/lkroll/Documents/Uni/Climate/tasmax_Amon_CCSM4_historical_r1i1p1_185001-200512.nc");
//            MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
//            BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MaxInfVarRecordMeasure());
//            VariableAlignment va = aligner.align();
//            System.out.println("Chosen Alignment:\n" + va);
//        } catch (IOException ex) {
//            ex.printStackTrace(System.err);
//            Assert.fail(ex.getMessage());
//        }
//    }
//
//    @Test
//    public void testTwoVars() {
//        try {
//            NetcdfFile ncfile = NetcdfFile.open("/Users/lkroll/Documents/Uni/Climate/tasminmax_Amon_EC-EARTH_historical_r2i1p1_195001-201212.nc");
//            MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
//            //BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MaxInfVarRecordMeasure());
//            BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MinFilesMeasure());
//            VariableAlignment va = aligner.align();
//            System.out.println("Chosen Alignment:\n" + va);
//        } catch (IOException ex) {
//            ex.printStackTrace(System.err);
//            Assert.fail(ex.getMessage());
//        }
//    }
    @Test
    public void testWriting() {
        try {
            String inPath = dir.getAbsolutePath() + "/in.nc";
            File f = new File(inPath);
            f.createNewFile();
            f.deleteOnExit();
            fg.generate(f);
            System.out.println("***** File generated: " + inPath + " *****");
            NetcdfFile ncfile = NetcdfFile.open(inPath);
            MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
            //BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MaxInfVarRecordMeasure());
            BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MinFilesMeasure());
            VariableAlignment va = aligner.align();
            System.out.println("***** Chosen Alignment:\n" + va + " *****");
            NetCDFWriter writer = new NetCDFWriter();
            System.out.println("***** Alignment complete. Writing... *****");
            writer.write(va, q);
            q.complete();
            System.out.println("***** Writing complete. *****");
            List<NetcdfFile> ncfiles = new LinkedList<>();
            Optional<File> fO = q.take();
            while (fO.isPresent()) {
                NetcdfFile ncf = NetcdfFile.open(fO.get().getAbsolutePath());
                ncfiles.add(ncf);
                // next
                fO = q.take();
            }
            System.out.println("***** Checking... *****");
            Assert.assertTrue(fg.checkBlocks(ncfiles));
            System.out.println("***** All checked out! *****");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
    }

}
