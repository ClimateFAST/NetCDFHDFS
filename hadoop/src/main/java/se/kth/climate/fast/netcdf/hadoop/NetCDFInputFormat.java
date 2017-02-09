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
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.FASTConstants;
import se.kth.climate.fast.netcdf.NetCDFConstants;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 * @param <T> value type
 */
public abstract class NetCDFInputFormat<T> extends FileInputFormat<Void, T> {

    static final Logger LOG = LoggerFactory.getLogger(NetCDFInputFormat.class);

    public static void addInputPath(Job job, Path path) throws IOException {
        FileInputFormat.addInputPath(job, path);
    }

    public abstract RecordReader<Void, T> getReader();

    @Override
    public RecordReader<Void, T> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        System.out.println("Got record reader request: " + is + " (" + is.getLength() + ")");
        return getReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // generate splits with one block each
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file : files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if (path.getName().endsWith(NetCDFConstants.SUFFIX)) { // read full file per record
                if (blkLocations.length == 1) { // in this case we can try to collocate with the block
                    splits.add(new CombineFileSplit(new Path[]{path}, new long[]{0}, new long[]{length}, blkLocations[0].getHosts()));
                } else { // otherwise, whatever
                    splits.add(new CombineFileSplit(new Path[]{path}, new long[]{length}));
                }
            } else if (path.getName().endsWith(FASTConstants.MERGED_SUFFIX)) {
                // for this type each block will contain one NetCDF file
                for (BlockLocation blk : blkLocations) {
                    splits.add(new FileSplit(path, blk.getOffset(), blk.getLength(),
                            blk.getHosts()));
                }
            }
        }
        return splits;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> input = super.listStatus(job);
        // filter out non-netcdf files
        ListIterator<FileStatus> it = input.listIterator();
        while (it.hasNext()) {
            FileStatus fs = it.next();
            if (fs.isFile()) {
                Path p = fs.getPath();
                if (!p.getName().endsWith(NetCDFConstants.SUFFIX) && !p.getName().endsWith(FASTConstants.MERGED_SUFFIX)) {
                    it.remove();
                }
            }
        }
        return input;
    }
}
