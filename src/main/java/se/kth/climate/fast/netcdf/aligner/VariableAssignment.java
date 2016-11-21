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

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 *
 * @author lkroll
 */
public class VariableAssignment {

    public final ImmutableSet<String> infVariables;
    public final ImmutableSet<String> dimensionVariables;
    public final ImmutableSet<String> boundsVariables;
    public final ImmutableSet<String> otherVariables;
    public final ImmutableSet<String> constants;

    private VariableAssignment(ImmutableSet<String> infVariables,
            ImmutableSet<String> dimensionVariables,
            ImmutableSet<String> boundsVariables,
            ImmutableSet<String> otherVariables,
            ImmutableSet<String> constants) {
        this.infVariables = infVariables;
        this.dimensionVariables = dimensionVariables;
        this.boundsVariables = boundsVariables;
        this.otherVariables = otherVariables;
        this.constants = constants;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VA(inf=");
        sb.append(infVariables);
        sb.append(",dim=");
        sb.append(dimensionVariables);
        sb.append(",bounds=");
        sb.append(boundsVariables);
        sb.append(",other=");
        sb.append(otherVariables);
        sb.append(",const=");
        sb.append(constants);
        sb.append(")");
        return sb.toString();
    }

    public static VariableAssignment assign(Set<String> infVariables,
            Set<String> dimensionVariables,
            Set<String> boundsVariables,
            Set<String> otherVariables,
            Set<String> constants) {
        return new VariableAssignment(ImmutableSet.copyOf(infVariables),
                ImmutableSet.copyOf(dimensionVariables),
                ImmutableSet.copyOf(boundsVariables),
                ImmutableSet.copyOf(otherVariables),
                ImmutableSet.copyOf(constants));
    }

    public static VariableAssignment assign(Set<String> dimensionVariables,
            Set<String> boundsVariables,
            Set<String> otherVariables,
            Set<String> constants) {
        return new VariableAssignment(ImmutableSet.of(),
                ImmutableSet.copyOf(dimensionVariables),
                ImmutableSet.copyOf(boundsVariables),
                ImmutableSet.copyOf(otherVariables),
                ImmutableSet.copyOf(constants));
    }

}
