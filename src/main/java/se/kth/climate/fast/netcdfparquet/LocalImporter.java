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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.math3.util.Pair;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author lkroll
 */
public class LocalImporter implements Runnable {

    private final NetcdfFile[] ncfiles;
    private final UserConfig uc;
    private RecordSinkFactory sinkFactory = new FakeSink.Factory();
    private MetaSinkFactory metaFactory = new StdSink.Factory();

    public LocalImporter(NetcdfFile[] ncfiles, UserConfig uc) {
        this.ncfiles = ncfiles;
        this.uc = uc;
    }

    public void prepare(String outputFileName, boolean force) {
        File outputFile = new File(outputFileName);
        File metaOutputFile = new File(outputFileName + ".meta");
        Util.checkFile(outputFile, force);
        Util.checkFile(metaOutputFile, force);
        LOG.info("Running in local mode and outputting to {} ({})", outputFileName, metaOutputFile);
        sinkFactory = new ParquetSink.Factory(
                new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath()));
        metaFactory = new AvroSink.FileFactory(metaOutputFile);
    }

    @Override
    public void run() {
        for (NetcdfFile ncfile : ncfiles) {
            System.out.println("Global Attrs: " + ncfile.getLocation());
            System.out.println(ncfile.getGlobalAttributes());
        }
        for (NetcdfFile ncfile : ncfiles) {
            System.out.println("Dimensions: " + ncfile.getLocation());
            System.out.println(ncfile.getDimensions());
        }
        for (NetcdfFile ncfile : ncfiles) {
            System.out.println("Variables: " + ncfile.getLocation());

            System.out.println(ncfile.getVariables());
        }

        Pair<Schema, AvroSchemaGenerator.MetaInfo> sbi = AvroSchemaGenerator.fromNetCDF(ncfiles[0]);
        Schema avroSchema = sbi.getFirst();
        AvroSchemaGenerator.MetaInfo bi = sbi.getSecond();
        System.out.println("Avro Schema:");
        System.out.println(avroSchema.toString(true));

        try (RecordSink rs = sinkFactory.create(avroSchema);
                MetaSink ms = metaFactory.create();) {
            List<Metadata> metas = new ArrayList<>();

            NCRecordGenerator recGen = new NCRecordGenerator(avroSchema, bi, rs, uc);
            for (NetcdfFile ncfile : ncfiles) {
                Metadata meta = recGen.prepare(ncfile);
                metas.add(meta);
                LOG.info("Metadata for {}: {}", ncfile.getLocation(), meta);
                int numRecords = recGen.generate(ncfile);
                LOG.info("Generated {} records for {}", numRecords, ncfile.getLocation());
            }
            Metadata finalMeta = MetadataBuilder.merge(metas);
            LOG.info("Final Metadata: {}", finalMeta);
            ms.sink(finalMeta);
        } catch (Exception ex) {
            LOG.error("Error during sinking", ex);
        }
    }

}
