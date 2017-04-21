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
package se.kth.climate.fast.netcdf.testing;

import se.kth.climate.fast.netcdf.MetaInfo;
import se.kth.climate.fast.netcdf.VariableMapping;
import ucar.ma2.DataType;
import ucar.nc2.Variable;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class TestMapping implements VariableMapping<Integer, Double> {

    @Override
    public String variable() {
        return FileGenerator.VAR;
    }

    @Override
    public DataType inputType() {
        return DataType.INT;
    }

    @Override
    public DataType outputType() {
        return DataType.DOUBLE;
    }

    @Override
    public MapperFactory<Integer, Double> prepare(MetaInfo mi) {
        System.out.println("Preparing Mapping for " + variable());
        return new TestFactory();
    }

    public class TestFactory implements VariableMapping.MapperFactory<Integer, Double> {

        @Override
        public Mapper<Integer, Double> mapper(Variable v) {
            System.out.println("Creating Mapper for " + v.toString());
            return new TestMapper();
        }

        @Override
        public VariableMapping<Integer, Double> mapping() {
            return TestMapping.this;
        }

    }

    public class TestMapper implements VariableMapping.ValueMapper<Integer, Double> {

        @Override
        public Double map(Integer value) {
            return 2.0 * (double) value;
        }

        @Override
        public VariableMapping<Integer, Double> mapping() {
            return TestMapping.this;
        }

    }
}
