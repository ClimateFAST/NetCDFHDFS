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
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class NetCDFFileReader extends RecordReader<Void, NCWritable> {

    static final Logger LOG = LoggerFactory.getLogger(NetCDFFileReader.class);
    // 
    private Optional<NCWritable> ncfileO = Optional.absent();
    private boolean loaded = false;
    private boolean read = false;
    private FSDataInputStream istream = null;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        LOG.info("{}: Got split: {}", this, is);
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
            istream = fs.open(p, (int) bs);
            //istream.seek(split.getStart());
            byte[] data = new byte[len];
            istream.readFully(split.getStart(), data);
//            int readBytes = 0;
//            do {
//                int res = istream.read(data, readBytes, len - readBytes);
//                if (res > -1) {
//                    readBytes += res;
//                } else {
//                    throw new IOException("End of stream reached before whole file was read!");
//                }
//            } while (readBytes < len);
            //LOG.info("Read {}bytes of {}bytes with {}*4 zeroes", new Object[]{len, fstat.getLen(), arrayZeroes(data)});
            NCWritable ncw = NCWritable.fromRaw(data, p.getName());
            //NetcdfFile ncfile = NetcdfFile.openInMemory(p.getName(), data);
            ncw.get().setTitle(p.getName()); // FIXME not really the right thing to put there
            ncfileO = Optional.of(ncw);
            loaded = true;
            LOG.info("Using {} ({} x {})", new Object[]{split.getPath(), fstat, fs.getDefaultBlockSize(p)});
        }
        if (is instanceof CombineFileSplit) {
            CombineFileSplit split = (CombineFileSplit) is;
            if (split.getNumPaths() == 1) {
                Path p = split.getPath(0);
                FileSystem fs = p.getFileSystem(tac.getConfiguration());
                FileStatus fstat = fs.getFileStatus(p);
                long llen = split.getLength(0);
                if (llen <= Integer.MAX_VALUE) {
                    int len = (int) llen;
                    long bs = fstat.getBlockSize();
                    istream = fs.open(p, (int) bs);
                    //istream.seek(split.getOffset(0));
                    byte[] data = new byte[len];
                    istream.readFully(split.getOffset(0), data);
//                    int readBytes = 0;
//                    do {
//                        int res = istream.read(data, readBytes, len - readBytes);
//                        if (res > -1) {
//                            readBytes += res;
//                        } else {
//                            throw new IOException("End of stream reached before whole file was read!");
//                        }
//                    } while (readBytes < len);
                    //LOG.info("Read {}bytes of {}bytes with {}*4 zeroes", new Object[]{len, fstat.getLen(), arrayZeroes(data)});
                    NCWritable ncw = NCWritable.fromRaw(data, p.getName());
                    //NetcdfFile ncfile = NetcdfFile.openInMemory(p.getName(), data);
                    ncw.get().setTitle(p.getName()); // FIXME not really the right thing to put there
                    ncfileO = Optional.of(ncw);
                    loaded = true;
                    LOG.info("Using {} ({} x {})", new Object[]{split.getPath(0), fstat, fs.getDefaultBlockSize(p)});
                } else {
                    LOG.error("This file is too large to be buffered in memory: {} ({}bytes)", p, llen);
                }
            } else {
                LOG.error("Only supporting single path per split for now, found {}", split.getNumPaths());
            }
        } else {
            LOG.error("Expected FileSplit, found {}", is.getClass());
        }
    }

//    private long arrayZeroes(byte[] data) {
//        long c = 0;
//        for (int i = 0; i < data.length; i++) {
//            if (data[i] == 0) {
//                c++;
//            }
//        }
//        return c;
//    }
    
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
    public NCWritable getCurrentValue() throws IOException, InterruptedException {
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
//        if (ncfileO.isPresent()) {
//            NetcdfFile ncfile = ncfileO.get();
//            ncfile.close();
//        }
        if (istream != null) {
            istream.close();
            istream = null;
        }
    }

}
