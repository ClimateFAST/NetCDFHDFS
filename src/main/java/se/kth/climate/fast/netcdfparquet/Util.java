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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;

/**
 *
 * @author lkroll
 */
public class Util {

    public static void checkFile(File f, boolean force) {
        if (f.exists()) {
            if (force) {
                LOG.info("Force option requested. Deleting exising output file {}.", f);
                if (!f.delete()) {
                    LOG.error("Could not delete {}!", f);
                    System.exit(1);
                }
            } else {
                LOG.error("Output file {} exists!", f);
                System.exit(1);
            }
        }
    }

    public static void checkFile(FileSystem fs, Path p, boolean force) throws IOException {
        if (fs.exists(p)) {
            if (force) {
                LOG.info("Force option requested. Deleting exising output file {}.", p);
                if (!fs.delete(p, true)) {
                    LOG.error("Could not delete {}!", p);
                    System.exit(1);
                }
            } else {
                LOG.error("Output file {} exists!", p);
                System.exit(1);
            }
        }
    }
}
