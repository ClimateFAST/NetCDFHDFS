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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class WorkQueue<T> {

    private final BlockingQueue<QueueValue<T>> pipe;

    public WorkQueue(int bufferSize) {
        pipe = new ArrayBlockingQueue<>(bufferSize);
    }

    public void put(T value) {
        try {
            pipe.put(new Value(value));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Optional<T> take() {
        try {
            QueueValue<T> v = pipe.take();
            if (v.isComplete()) {
                return Optional.absent();
            } else {
                return Optional.of(v.get());
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void complete() {
        try {
            pipe.put(new Complete());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    interface QueueValue<T> {

        T get();

        boolean isComplete();
    }

    class Complete implements QueueValue<T> {

        @Override
        public T get() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean isComplete() {
            return true;
        }

    }

    class Value implements QueueValue<T> {

        private final T value;

        Value(T value) {
            this.value = value;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public boolean isComplete() {
            return false;
        }

    }
}
