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

import com.google.common.base.Optional;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class NetCDFFileReader extends RecordReader<Void, NetcdfFile> {

    static final Logger LOG = LoggerFactory.getLogger(NetCDFFileReader.class);
    // 
    private Optional<NetcdfFile> ncfileO = Optional.absent();
    private boolean loaded = false;
    private boolean read = false;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        LOG.debug("{}: Got split: {}", this, is);
        if (is instanceof FileSplit) {
            FileSplit split = (FileSplit) is;
            Path p = split.getPath();
            FileSystem fs = p.getFileSystem(tac.getConfiguration());
            FileStatus fstat = fs.getFileStatus(p);
            long bs = fstat.getBlockSize();
            if (split.getLength() > bs) {
                throw new IOException("NetCDF file is not appropriately block aligned! Cannot guarantee corrent read. (len=" + split.getLength() + ", bs=" + bs + ")");
            }
            int len = (int) split.getLength(); //fstat.getLen();
            FSDataInputStream istream = fs.open(p, (int) bs);
            istream.seek(split.getStart());
            byte[] data = new byte[len];
            istream.read(data);
            NetcdfFile ncfile = NetcdfFile.openInMemory(p.getName(), data);
            ncfile.setTitle(p.getName());
            ncfileO = Optional.of(ncfile);
            loaded = true;
            LOG.info("Using {} ({} x {})", new Object[]{split.getPath(), fstat, fs.getDefaultBlockSize(p)});
        } else {
            LOG.error("Expected FileSplit, found {}", is.getClass());
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (loaded && ncfileO.isPresent() && !read) {
            read = true;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Void getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public NetcdfFile getCurrentValue() throws IOException, InterruptedException {
        if (ncfileO.isPresent()) {
            return ncfileO.get();
        } else {
            return null;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float status = 0.0f;
        if (loaded) {
            status += 1.0f;
        }
        if (read) {
            status += 1.0f;
        }
        return status / 2.0f;
    }

    @Override
    public void close() throws IOException {
        if (ncfileO.isPresent()) {
            NetcdfFile ncfile = ncfileO.get();
            ncfile.close();
        }
    }

}
