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

import ucar.ma2.DataType;
import ucar.nc2.Variable;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public interface VariableMapping<I, O> {
    public interface Mapper<I, O> {
        public O map(I value);
        public VariableMapping<I, O> mapping();
    }
    public interface ValueMapper<I, O> extends Mapper<I, O> {
        
    }
//    public interface IndexValueMapper<I, O> extends Mapper<I, O> {
//        
//    }
    public interface MapperFactory<I, O> {
        public Mapper<I, O> mapper(Variable v);
        public VariableMapping<I, O> mapping();
    }
    
    public String variable();
    public DataType inputType();
    public DataType outputType();
    public MapperFactory<I, O> prepare(MetaInfo mi);
}
