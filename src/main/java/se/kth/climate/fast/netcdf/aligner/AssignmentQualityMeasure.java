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

import se.kth.climate.fast.netcdf.MetaInfo;

/**
 *
 * @author lkroll
 */
public interface AssignmentQualityMeasure {

    /**
     * Generate a score for a certain variable assignment, that the solver uses
     * to find the best assignment.
     * <p>
     * @param va
     * @param metaInfo
     * @return
     */
    public double score(VariableAlignment va, MetaInfo metaInfo);

    /**
     * A short title used for selection via config or commandline arguments
     * <p>
     * @return the title
     */
    public String title();
    
    /**
     * A longer description that the explains what the measure does.
     * 
     * @return the description
     */
    public String description();
}
