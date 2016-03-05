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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import se.kth.climate.fast.common.Metadata;

/**
 *
 * @author lkroll
 */
public class StdSink {

    public static class Factory implements MetaSinkFactory {

        @Override
        public MetaSink create() throws IOException {
            ReflectDatumWriter< Metadata> reflectDatumWriter = new ReflectDatumWriter<>(Metadata.AVRO);
            DataFileWriter< Metadata> writer = new DataFileWriter<>(reflectDatumWriter).create(Metadata.AVRO, System.out);
            return new AvroSink(writer);
        }

    }
}
