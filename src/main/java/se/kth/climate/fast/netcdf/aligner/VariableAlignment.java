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
package se.kth.climate.fast.netcdf.aligner;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import org.javatuples.Pair;

/**
 *
 * @author lkroll
 */
public class VariableAlignment implements Iterable<Pair<VariableAssignment, VariableFit>> {

    public final ImmutableList<VariableAssignment> assignments;
    public final ImmutableList<VariableFit> fits;

    private VariableAlignment(ImmutableList<VariableAssignment> assignments, ImmutableList<VariableFit> fits) {
        this.assignments = assignments;
        this.fits = fits;
    }

    @Override
    public Iterator<Pair<VariableAssignment, VariableFit>> iterator() {
        return new  Iterator<Pair<VariableAssignment, VariableFit>>() {

            private int pos = 0;
            
            @Override
            public boolean hasNext() {
                return pos < assignments.size();
            }

            @Override
            public Pair<VariableAssignment, VariableFit> next() {
                Pair<VariableAssignment, VariableFit> pvv = Pair.with(assignments.get(pos), fits.get(pos));
                pos++;
                return pvv;
            }
        };
    }

    public static VariableAlignment of(List<Pair<VariableAssignment, VariableFit>> vavfs) {
        final ImmutableList.Builder<VariableAssignment> vas = ImmutableList.builder();
        final ImmutableList.Builder<VariableFit> vfs = ImmutableList.builder();
        vavfs.forEach((vavf) -> {
            vas.add(vavf.getValue0());
            vfs.add(vavf.getValue1());
        });
        return new VariableAlignment(vas.build(), vfs.build());
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VariableAlignment(\n");
        for (int i = 0; i < assignments.size(); i++) {
            sb.append(assignments.get(i));
            sb.append(" ==> ");
            sb.append(fits.get(i));
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }
}
