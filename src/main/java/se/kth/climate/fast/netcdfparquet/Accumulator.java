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

import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.ma2.DataType;

/**
 *
 * @author lkroll
 * @param <T>
 */
public abstract class Accumulator<T> implements Offsetter {

    public abstract void update(T val);

    public static Accumulator forDataType(DataType type) {
        switch (type) {
            case DOUBLE:
                return new DoubleAcc();
            case FLOAT:
                return new FloatAcc();
            case INT:
                return new IntAcc();
            case LONG:
                return new LongAcc();
            default:
                LOG.warn("Type {} has no accumulator!", type);
                return null;
        }
    }

    public static class DoubleAcc extends Accumulator<Double> {

        private double acc = 0.0;

        @Override
        public void update(Double val) {
            acc += val;
        }

        @Override
        public Double offset(Object original) {
            Double orig = (Double) original;
            return orig + acc;
        }

    }

    public static class FloatAcc extends Accumulator<Float> {

        private float acc = 0.0f;

        @Override
        public void update(Float val) {
            acc += val;
        }

        @Override
        public Float offset(Object original) {
            Float orig = (Float) original;
            return orig + acc;
        }

    }

    public static class IntAcc extends Accumulator<Integer> {

        private int acc = 0;

        @Override
        public void update(Integer val) {
            this.acc += val;
        }

        @Override
        public Integer offset(Object original) {
            Integer orig = (Integer) original;
            return this.acc + orig;
        }

    }

    public static class LongAcc extends Accumulator<Long> {

        private long acc = 0l;

        @Override
        public void update(Long val) {
            this.acc += val;
        }

        @Override
        public Long offset(Object original) {
            Long orig = (Long) original;
            return this.acc + orig;
        }

    }
}
