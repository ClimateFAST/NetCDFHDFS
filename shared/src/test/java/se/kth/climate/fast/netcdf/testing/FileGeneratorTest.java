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

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.DEFAULT)
@net.jcip.annotations.NotThreadSafe
public class FileGeneratorTest {

//    public FileGeneratorTest() {
//    }
    private static final long MB = 1024 * 1024;
    private final File dir;

    {
        dir = Files.createTempDir();
        dir.deleteOnExit();
        System.out.println("Generating instance of test" + dir.getAbsolutePath());
    }

    @Test
    public void smallFileTest() {
        System.out.println("Running test case small!");
        try {
            long size = 10 * MB;
            FileGenerator fg = new FileGenerator(size);
            Path p = dir.toPath();
            Path fp = p.resolve("smallFile.nc");
            File f = fp.toFile();
            System.out.println("Creating file: " + f.getAbsolutePath());
            f.createNewFile();
            f.deleteOnExit();
            fg.generate(f);
            System.out.println("Generated file: " + f.getAbsolutePath());
            NetcdfFile ncfile = NetcdfFile.open(f.getAbsolutePath());
            Assert.assertTrue("File did not check out!", fg.check(ncfile));
            ncfile.close();
            System.out.println("Checked file: " + f.getAbsolutePath());
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
        System.out.println("Finished test case small!");
    }

    @Test
    public void largeFileTest() {
        System.out.println("Running test case large!");
        try {
            long size = 200 * MB;
            FileGenerator fg = new FileGenerator(size);
            Path p = dir.toPath();
            Path fp = p.resolve("largeFile.nc");
            File f = fp.toFile();
            System.out.println("Creating file: " + f.getAbsolutePath());
            f.createNewFile();
            f.deleteOnExit();
            fg.generate(f);
            System.out.println("Generated file: " + f.getAbsolutePath());
            NetcdfFile ncfile = NetcdfFile.open(f.getAbsolutePath());
            Assert.assertTrue("File did not check out!", fg.check(ncfile));
            ncfile.close();
            System.out.println("Checked file: " + f.getAbsolutePath());
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
        System.out.println("Finished test case large!");
    }

    @Test
    public void blockedFileTest() {
        System.out.println("Running test case blocked!");
        try {
            long size = 200 * MB;
            FileGenerator fg = new FileGenerator(size);
            List<Path> files = fg.generateBlocks(dir, 3);
            System.out.println("Generated files: " + Iterables.toString(files));
            List<NetcdfFile> ncfiles = new ArrayList<>(files.size());
            for (Path p : files) {
                File f = p.toFile();
                f.deleteOnExit();
                NetcdfFile ncfile = NetcdfFile.open(p.toString());
                ncfiles.add(ncfile);
            }
            Assert.assertTrue("Files did not check out!", fg.checkBlocks(ncfiles));
            for (NetcdfFile ncfile : ncfiles) {
                ncfile.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Assert.fail(ex.getMessage());
        }
        System.out.println("Finished test case blocked!");
    }
}
