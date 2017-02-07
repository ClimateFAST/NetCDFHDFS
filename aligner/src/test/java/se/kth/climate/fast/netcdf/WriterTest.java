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

import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.netcdf.testing.FileGenerator;

/**
 *
 * @author lkroll
 */
public class WriterTest {

    public WriterTest() {
    }

    private WorkQueue<File> q;
    private FileGenerator fg;

    @Before
    public void setUp() {
        q = new WorkQueue<File>(1000);
        fg = new FileGenerator(10000000);
    }

    // handled in the AlignerTest
//    @Test
//    public void testWriter() {
//        NetCDFWriter writer = new NetCDFWriter();
////        try {
////            writer.write();
////        } catch (IOException ex) {
////            ex.printStackTrace(System.err);
////            Assert.fail(ex.getMessage());
////        }
//    }
    @Test
    public void testMetaData() throws IOException {
        NetCDFWriter writer = new NetCDFWriter();
        Metadata m = fg.generateMeta();
        writer.writeMeta(m, q);
        File f = q.take().get(); // I know it must be in there due to sync write
        f.deleteOnExit();
        Assert.assertTrue(f.canRead());
        String metaS = new String(Files.readAllBytes(f.toPath()));
        Gson gson = new Gson();
        Metadata m2 = gson.fromJson(metaS, Metadata.class);
        Assert.assertTrue(fg.checkMeta(m));
    }
}
