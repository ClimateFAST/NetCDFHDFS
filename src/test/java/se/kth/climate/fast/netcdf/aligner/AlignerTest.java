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

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import se.kth.climate.fast.netcdf.MetaInfo;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author lkroll
 */
public class AlignerTest {

    public static final long BLOCK_SIZE = 64l * 1000l * 1000l;

    public AlignerTest() {
    }

    @Test
    public void testSmall() {
        try {
            NetcdfFile ncfile = NetcdfFile.open("/Users/lkroll/Documents/Uni/Climate/tasmax_Amon_CCSM4_historical_r1i1p1_185001-200512.nc");
            MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
            BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MaxInfVarRecordMeasure());
            VariableAlignment va = aligner.align();
            System.out.println("Chosen Alignment:\n" + va);
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testTwoVars() {
        try {
            NetcdfFile ncfile = NetcdfFile.open("/Users/lkroll/Documents/Uni/Climate/tasminmax_Amon_EC-EARTH_historical_r2i1p1_195001-201212.nc");
            MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
            //BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MaxInfVarRecordMeasure());
            BlockAligner aligner = new BlockAligner(BLOCK_SIZE, mInfo, new MinFilesMeasure());
            VariableAlignment va = aligner.align();
            System.out.println("Chosen Alignment:\n" + va);
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
    }

}
