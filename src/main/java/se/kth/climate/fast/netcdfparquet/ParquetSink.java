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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 *
 * @author lkroll
 * @deprecated As of 0.3-SNAPSHOT the whole NetCDFParquet API is replaced with
 * NetCDF Alignment.
 */
@Deprecated
public class ParquetSink implements RecordSink {

    static final CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
    static final int blockSize = 256 * 1024 * 1024;
    static final int pageSize = 64 * 1024;

    private final Schema avroSchema;
    //private final MessageType parquetSchema;
    private ParquetWriter parquetWriter;

    public ParquetSink(Schema avroSchema) {
        //parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        this.avroSchema = avroSchema;
    }

    public void open(Path outputPath, boolean useDict) throws IOException {
//        AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
//        parquetWriter = new ParquetWriter(outputPath,
//                writeSupport, compressionCodec, blockSize, pageSize);
        parquetWriter = new AvroParquetWriter(outputPath, avroSchema, compressionCodec, blockSize, pageSize, useDict);
    }

    @Override
    public void close() throws Exception {
        parquetWriter.close();
    }

    @Override
    public void sink(GenericContainer record) throws IOException {
        parquetWriter.write(record);
    }

    public static class Factory implements RecordSinkFactory {

        private final Path outputFile;
        private final boolean noDict;

        public Factory(Path outputFile, boolean noDict) {
            this.outputFile = outputFile;
            this.noDict = noDict;
        }

        @Override
        public RecordSink create(Schema avroSchema) throws IOException {
            ParquetSink ps = new ParquetSink(avroSchema);
            ps.open(outputFile, !noDict);
            return ps;
        }
    }
}
