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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericContainer;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;

/**
 *
 * @author lkroll
 * @deprecated As of 0.3-SNAPSHOT the whole NetCDFParquet API is replaced with
 * NetCDF Alignment.
 */
@Deprecated
public class DecoupledSink implements RecordSink, Runnable {

    private final RecordSink actualSink;
    private final ArrayBlockingQueue<GenericContainer> recordQ = new ArrayBlockingQueue<>(10000);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private Thread thread = null;

    private DecoupledSink(RecordSink actualSink) {
        this.actualSink = actualSink;
    }

    public static DecoupledSink decouple(RecordSink actualSink) {
        DecoupledSink ds = new DecoupledSink(actualSink);
        Thread t = new Thread(ds);
        ds.thread = t;
        t.start();
        return ds;
    }

    @Override
    public void sink(GenericContainer record) throws IOException {
        while (true) { // retry until it works
            try {
                recordQ.put(record);
                return;
            } catch (InterruptedException ex) {
                LOG.warn("DecoupledSink was interrupted!", ex);
            }
        }
    }

    @Override
    public void close() throws Exception {
        while (!recordQ.isEmpty()) {
            Thread.sleep(100);
        }
        stopped.set(true);
        if (thread != null) {
            thread.join();
        }
        actualSink.close();
    }

    @Override
    public void run() {
        while (!stopped.get()) {
            try {
                GenericContainer record = recordQ.poll(100, TimeUnit.MILLISECONDS);
                if (record != null) {
                    actualSink.sink(record);
                }
            } catch (InterruptedException ex) {
                LOG.warn("DecoupledSink was interrupted!", ex);
            } catch (IOException ex) {
                LOG.error("Sink encountered an error! Can't recover -> shutting down", ex);
                System.exit(1);
            }
        }
    }

}
