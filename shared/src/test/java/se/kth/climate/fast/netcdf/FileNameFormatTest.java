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
package se.kth.climate.fast.netcdf;

import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
@RunWith(JUnit4.class)
public class FileNameFormatTest {

    private final int records = 100;
    private final String splitVar = "vS";
    private final List<String> vars = new ArrayList<String>();

    {
        vars.add("v1");
        vars.add("v2");
        vars.add("v3");
        //vars.add(splitVar);
    }

    @Test
    public void testMinimalFormat() {
        FileInfo fi = new FileInfo(vars);
        String fiS = FileNameFormat.serialise(fi);
        System.out.println("Generated name: " + fiS);
        FileInfo fi2 = FileNameFormat.deserialise(fiS);
        Assert.assertThat(fi2.vars, Matchers.containsInAnyOrder(vars.toArray()));
        Assert.assertFalse(fi2.splitVar.isPresent());
        Assert.assertFalse(fi2.drange.isPresent());
        Assert.assertFalse(fi2.trange.isPresent());
    }
    
    @Test
    public void testNoneFormat() {
        FileInfo fi = new FileInfo(vars, splitVar);
        String fiS = FileNameFormat.serialise(fi);
        System.out.println("Generated name: " + fiS);
        FileInfo fi2 = FileNameFormat.deserialise(fiS);
        Assert.assertThat(fi2.vars, Matchers.containsInAnyOrder(vars.toArray()));
        Assert.assertEquals(splitVar, fi2.splitVar.get());
        Assert.assertFalse(fi2.drange.isPresent());
        Assert.assertFalse(fi2.trange.isPresent());
    }

    @Test
    public void testDimFormat() {
        DimensionRange dr = new DimensionRange(splitVar, 0, records, false);
        FileInfo fi = new FileInfo(vars, dr);
        String fiS = FileNameFormat.serialise(fi);
        System.out.println("Generated name: " + fiS);
        FileInfo fi2 = FileNameFormat.deserialise(fiS);
        Assert.assertThat(fi2.vars, Matchers.containsInAnyOrder(vars.toArray()));
        Assert.assertEquals(splitVar, fi2.splitVar.get());
        Assert.assertTrue(fi2.drange.isPresent());
        Assert.assertFalse(fi2.trange.isPresent());
        DimensionRange dr2 = fi2.drange.get();
        Assert.assertEquals(0, dr2.start);
        Assert.assertEquals(records, dr2.end);
    }

    @Test
    public void testFullFormat() {
        // double
        TypedRange tr = TypedRange.with(0.0, 1920.4);
        FileInfo fi2 = generateAndCheck(tr);
        TypedRange tr2 = fi2.trange.get();
        Assert.assertEquals("Doubles should work.", tr.getDouble(), tr2.getDouble());
        // float
        tr = TypedRange.with(0.0f, 1920.4f);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Floats should work.", tr.getFloat(), tr2.getFloat());
        // long
        tr = TypedRange.with(0l, 1920l);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Longs should work.", tr.getLong(), tr2.getLong());
        // int
        tr = TypedRange.with(0, 1920);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Ints should work.", tr.getInt(), tr2.getInt());
        // short
        tr = TypedRange.with((short) 0, (short) 1920);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Shorts should work.", tr.getShort(), tr2.getShort());
        // byte
        tr = TypedRange.with((byte) 0, (byte) 120);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Bytes should work.", tr.getByte(), tr2.getByte());
        // char
        tr = TypedRange.with('a', 'v');
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Chars should work.", tr.getChar(), tr2.getChar());
        // boolean
        tr = TypedRange.with(true, false);
        fi2 = generateAndCheck(tr);
        tr2 = fi2.trange.get();
        Assert.assertEquals("Booleans should work.", tr.getBoolean(), tr2.getBoolean());
    }

    private FileInfo generateAndCheck(TypedRange tr) {
        DimensionRange dr = new DimensionRange(splitVar, 0, records, false);
        FileInfo fi = new FileInfo(vars, tr, dr);
        String fiS = FileNameFormat.serialise(fi);
        System.out.println("Generated name: " + fiS);
        FileInfo fi2 = FileNameFormat.deserialise(fiS);
        Assert.assertThat(fi2.vars, Matchers.containsInAnyOrder(vars.toArray()));
        Assert.assertEquals(splitVar, fi2.splitVar.get());
        Assert.assertFalse(fi2.drange.isPresent());
        Assert.assertTrue(fi2.trange.isPresent());
        return fi2;
    }
}
