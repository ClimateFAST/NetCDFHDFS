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
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

/**
 *
 * @author lkroll
 */
public class ParquetSink implements RecordSink {

    static final CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
    static final int blockSize = 256 * 1024 * 1024;
    static final int pageSize = 64 * 1024;

    private final Schema avroSchema;
    private final MessageType parquetSchema;
    private ParquetWriter parquetWriter;

    public ParquetSink(Schema avroSchema) {
        this.avroSchema = avroSchema;
        parquetSchema = new AvroSchemaConverter().convert(avroSchema);

    }

    public void open(Path outputPath) throws IOException {
        AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
        parquetWriter = new ParquetWriter(outputPath,
                writeSupport, compressionCodec, blockSize, pageSize);
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

        public Factory(Path outputFile) {
            this.outputFile = outputFile;
        }

        @Override
        public RecordSink create(Schema avroSchema) throws IOException {
            ParquetSink ps = new ParquetSink(avroSchema);
            ps.open(outputFile);
            return ps;
        }
    }
}
