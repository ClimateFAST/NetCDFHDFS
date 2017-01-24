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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.netcdf.aligner.AssignmentQualityMeasure;
import se.kth.climate.fast.netcdf.aligner.BlockAligner;
import se.kth.climate.fast.netcdf.aligner.MeasureRegister;
import se.kth.climate.fast.netcdf.aligner.VariableAlignment;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class LocalImporter implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LocalImporter.class);

    private final NetcdfFile[] ncfiles;
    private final Config conf;
    private final File target;

    public LocalImporter(NetcdfFile[] ncfiles, Config conf, File target) {
        this.ncfiles = ncfiles;
        this.conf = conf;
        this.target = target;
    }

    @Override
    public void run() {
        long blockSize = conf.getBytes("nchdfs.blockSize");
        AssignmentQualityMeasure aqm = MeasureRegister.fromConfig(conf);
        LocalSink ls = new LocalSink();
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture lsF = executor.submit(ls);
        try {
            for (NetcdfFile ncfile : ncfiles) {
                LOG.info("Processing input file {}", ncfile.getLocation());
                MetaInfo mInfo = MetaInfo.fromNetCDF(ncfile);
                BlockAligner aligner = new BlockAligner(blockSize, mInfo, aqm);
                VariableAlignment va = aligner.align();
                LOG.info("Chosen alignment: {}", va);
                NetCDFWriter writer = new NetCDFWriter();
                writer.write(va, ls.progressPipe);
                LOG.info("Finished input file: {}", ncfile.getLocation());
            }
        } catch (IOException ex) {
            LOG.error("Error during processing.", ex);
            throw new RuntimeException(ex);
        }
        ls.progressPipe.complete();
        try {
            lsF.get();
            LOG.info("Processing of all files complete.");
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error("Error during processing.", ex);
            throw new RuntimeException(ex);
        }
    }

    public class LocalSink implements Runnable {

        final WorkQueue<File> progressPipe = new WorkQueue<>(conf.getInt("nchdfs.bufferSize"));

        @Override
        public void run() {
            while (true) {
                try {
                    Optional<File> fo = progressPipe.take();
                    if (fo.isPresent()) {
                        File f = fo.get();
                        FileUtils.moveFileToDirectory(f, target, false);
                    } else {
                        return;
                    }
                } catch (IOException ex) {
                    LOG.error("Error while moving file.", ex);
                }
            }
        }
    }
}
