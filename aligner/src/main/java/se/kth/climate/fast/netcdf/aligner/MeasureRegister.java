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
package se.kth.climate.fast.netcdf.aligner;

import com.typesafe.config.Config;
import java.util.HashMap;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public abstract class MeasureRegister {

    private static final HashMap<String, AssignmentQualityMeasure> measures = new HashMap<>();

    static {
        AssignmentQualityMeasure aqm = new MaxInfVarRecordMeasure();
        measures.put(aqm.title(), aqm);
        aqm = new MinFilesMeasure();
        measures.put(aqm.title(), aqm);
    }

    public static AssignmentQualityMeasure fromConfig(Config conf) {
        String measure = conf.getString("nchdfs.assignmentMeasure");
        AssignmentQualityMeasure aqm = measures.get(measure);
        if (aqm == null) {
            throw new RuntimeException("Unkown assignment measure value: " + measure);
        }
        return aqm;
    }

    public static void listMeasures(StringBuilder sb) {
        for (AssignmentQualityMeasure aqm : measures.values()) {
            sb.append(aqm.title());
            sb.append(" -> ");
            sb.append(aqm.description());
            sb.append("\n");
        }
    }
}
