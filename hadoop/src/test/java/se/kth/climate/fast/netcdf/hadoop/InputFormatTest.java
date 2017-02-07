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

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.kth.climate.fast.netcdf.testing.FileGenerator;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
@RunWith(JUnit4.class)
public class InputFormatTest {

    private static final long FSIZE = 200 * 1024 * 1024;

    public InputFormatTest() {
    }

    private File dir;
    private Path out;
    private FileGenerator fgen;

    @Before
    public void setUp() throws IOException {
        dir = Files.createTempDir();
        dir.deleteOnExit();
        fgen = new FileGenerator(FSIZE);
        List<java.nio.file.Path> files = fgen.generateBlocks(dir, 7);
        for (java.nio.file.Path p : files) {
            File f = p.toFile();
            f.deleteOnExit();
        }
        out = (new Path(dir.getAbsolutePath())).suffix("/output.txt");
        System.out.println("Generated files: " + Iterables.toString(files));
    }

    @Test
    public void testMapReduce() {

        try {
            Configuration conf = new Configuration();
            String sep = ":";
            conf.set(TextOutputFormat.SEPERATOR, sep);
            Job job = Job.getInstance(conf, "input format test");
            job.setJarByClass(InputFormatTest.class);
            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CountSumWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CountSumWritable.class);
            FileInputFormat.addInputPath(job, new Path(dir.toURI()));
            job.setInputFormatClass(NetCDFFileFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, out);
            boolean res = job.waitForCompletion(true);
            Assert.assertTrue(res);
            File fout = new File(out.suffix("/part-r-00000").toString());
            System.out.println("Reading " + fout.getAbsolutePath());
            try (BufferedReader in = new BufferedReader(new FileReader(fout))) {
                String line = in.readLine();
                System.out.println("Read line \"" + line + "\" sep=" + sep);
                String[] kv = line.split(sep);
                Assert.assertEquals(3, kv.length);
                int count = Integer.parseInt(kv[1]);
                long sum = Long.parseLong(kv[2]);
                Assert.assertTrue(fgen.checkBlockSum(sum, count));
            }
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
    }
}
