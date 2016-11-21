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

import com.google.gson.Gson;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import se.kth.climate.fast.common.Metadata;

/**
 *
 * @author lkroll
 */
public class GsonSink implements MetaSink {

    private final OutputStreamWriter writer;
    private final Gson gson = new Gson();

    GsonSink(OutputStreamWriter writer) {
        this.writer = writer;
    }
    
    @Override
    public void sink(Metadata meta) throws IOException {
        String json = gson.toJson(meta);
        writer.write(json);
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
    
    
    public static class FileFactory implements MetaSinkFactory {

        private final File target;

        public FileFactory(File target) {
            this.target = target;
        }

        @Override
        public MetaSink create() throws IOException {
            FileWriter writer = new FileWriter(target);
            return new GsonSink(writer);
        }

    }

    public static class StreamFactory implements MetaSinkFactory {

        private final OutputStream target;

        public StreamFactory(OutputStream target) {
            this.target = target;
        }

        @Override
        public MetaSink create() throws IOException {
            OutputStreamWriter writer = new OutputStreamWriter(target);
            return new GsonSink(writer);
        }

    }
}
