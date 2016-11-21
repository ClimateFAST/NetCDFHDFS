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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.nc2.NetcdfFile;

/**
 *
 * @author lkroll
 * @deprecated As of 0.3-SNAPSHOT the whole NetCDFParquet API is replaced with
 * NetCDF Alignment.
 */
@Deprecated
public class HDFSImporter implements Runnable {

    private final NetcdfFile[] ncfiles;
    private final UserConfig uc;
    private RecordSinkFactory sinkFactory = new FakeSink.Factory();
    private MetaSinkFactory metaFactory = new StdSink.Factory();
    private UserGroupInformation ugi;
    private FileSystem fs;

    public HDFSImporter(NetcdfFile[] ncfiles, UserConfig uc) {
        this.ncfiles = ncfiles;
        this.uc = uc;
    }

    public void prepare(final String hdfs, final String hdfsPath, final boolean force) throws IOException, URISyntaxException, InterruptedException {
        System.setProperty("hadoop.home.dir", "/");
        ugi = UserGroupInformation.createRemoteUser("Test__meb10000");

        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
                LOG.debug("Adding HDFS Config");
                Configuration conf = new Configuration();
                //conf.set("hadoop.job.ugi", "Admin");

                LOG.debug("Connecting to HDFS...");
                fs = FileSystem.get(new URI(hdfs), conf);

                LOG.debug("Getting file status...");
                FileStatus[] status = fs.listStatus(new Path("/Projects/Test"));
                for (int i = 0; i < status.length; i++) {
                    LOG.info("In Path: {}", status[i].getPath());
                }

                Path outputFile = new Path(hdfs + hdfsPath);
                Path metaFile = outputFile.suffix("meta");
                sinkFactory = new ParquetSink.Factory(outputFile, uc.noDict);
                OutputStream os = fs.create(metaFile,
                        new Progressable() {
                            @Override
                            public void progress() {
                                LOG.debug("Sinking Metadata...");
                            }
                        });
                metaFactory = new AvroSink.StreamFactory(os);
                return null;
            }
        });
    }

    @Override
    public void run() {
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                @Override
                public Void run() throws Exception {
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
                    } finally {
                        fs.close();
                    }
                    return null;
                }
            });
        } catch (IOException ex) {
            LOG.error("Blah", ex);
        } catch (InterruptedException ex) {
            LOG.error("Blah", ex);
        }
    }

}
