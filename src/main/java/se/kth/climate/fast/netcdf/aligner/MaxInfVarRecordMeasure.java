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

import org.javatuples.Pair;
import se.kth.climate.fast.netcdf.MetaInfo;

/**
 *
 * @author lkroll
 */
public class MaxInfVarRecordMeasure implements AssignmentQualityMeasure {

    @Override
    public double score(VariableAlignment va, MetaInfo metaInfo) {
        double score = 0.0;
        for (Pair<VariableAssignment, VariableFit> pvv : va) {
            final VariableAssignment vass = pvv.getValue0();
            final VariableFit vf = pvv.getValue1();
            for (String v : vass.infVariables) {
                score += (double) vf.recordsForVar.get(v);
            } // reward large number of records of inf var
            score -= vf.numberOfFiles; // penalise large number of files
        }
        return score;
    }

    @Override
    public String title() {
        return "MIVRM";
    }

    @Override
    public String description() {
        return "Try to maximise the number of records of some unlimited dimension per file. This can lead to assignments where unrelated variable groups are split into different files.";
    }

}
