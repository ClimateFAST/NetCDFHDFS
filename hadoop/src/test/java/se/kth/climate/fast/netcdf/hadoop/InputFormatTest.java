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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class InputFormatTest {

    public InputFormatTest() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void testMapReduce() {

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "input format test");
            job.setJarByClass(InputFormatTest.class);
            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NetcdfFile.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://bbc6.sics.se:26801/FAST/tasminmax_Amon_EC_EARTH_historical_r2i1p1_195001_201212"));
            job.setInputFormatClass(NetCDFFileFormat.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
    }
}
