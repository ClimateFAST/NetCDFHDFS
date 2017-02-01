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
package se.kth.climate.fast.netcdf.hdfs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import se.kth.climate.fast.netcdf.MetaInfo;
import se.kth.climate.fast.netcdf.NetCDFWriter;
import se.kth.climate.fast.netcdf.aligner.AssignmentQualityMeasure;
import se.kth.climate.fast.netcdf.aligner.BlockAligner;
import se.kth.climate.fast.netcdf.aligner.MeasureRegister;
import se.kth.climate.fast.netcdf.aligner.VariableAlignment;
import se.kth.climate.fast.netcdf.metadata.MetaConverter;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class HDFSImporter implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSImporter.class);

    private final NetcdfFile[] ncfiles;
    private Config conf;
    private final HDFSSink sink;

    public HDFSImporter(NetcdfFile[] ncfiles, Config conf, String hdfsUser, String hdfsUrl) {
        this.ncfiles = ncfiles;
        this.conf = conf;
        this.sink = HDFSSink.getBasic(hdfsUrl, hdfsUser, conf);
    }

    @Override
    public void run() {
        long blockSize = conf.getBytes("nchdfs.blockSize");
        AssignmentQualityMeasure aqm = MeasureRegister.fromConfig(conf);
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture lsF = executor.submit(sink);
        List<Metadata> metas = new ArrayList<>(ncfiles.length);
        NetCDFWriter writer = new NetCDFWriter();
        try {
            for (NetcdfFile ncfile : ncfiles) {
                LOG.info("Processing input file {}", ncfile.getLocation());
                MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
                Metadata meta = MetaConverter.convert(ncfile, mInfo);
                metas.add(meta);
                LOG.info("Metadata for file {}:\n   {}", ncfile.getLocation(), meta);
                BlockAligner aligner = new BlockAligner(blockSize, mInfo, aqm);
                VariableAlignment va = aligner.align();
                LOG.info("Chosen alignment: {}", va);
                writer.write(va, sink.progressPipe);
                LOG.info("Finished input file: {}", ncfile.getLocation());
            }
            Metadata metameta = MetadataBuilder.merge(metas);
            writer.writeMeta(metameta, sink.progressPipe);
            LOG.info("Wrote Metadata.");
        } catch (IOException ex) {
            LOG.error("Error during processing.", ex);
            throw new RuntimeException(ex);
        }
        sink.progressPipe.complete();
        try {
            lsF.get();
            LOG.info("Processing of all files complete.");
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error("Error during processing.", ex);
            throw new RuntimeException(ex);
        }
    }

    public boolean prepare() {
        if (!(sink.canConnect() && sink.rootFolderExists())) {
            return false;
        }

        long blockSize = sink.rootFolderBlockSize();
        if (blockSize > 0) {
            LOG.info("Got block size of {} from HDFS namenode.", blockSize);
            conf = conf.withValue("nchdfs.blockSize", ConfigValueFactory.fromAnyRef(blockSize, "HDFS info"));
            String title = conf.getString("nchdfs.title");
            return sink.createProjectFolder(title);
        } else {
            return false;
        }
    }
}
