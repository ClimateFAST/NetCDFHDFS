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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public abstract class FileNameFormat {

    static final Logger LOG = LoggerFactory.getLogger(FileNameFormat.class);
    static final String SEP = "_";
    static final String DIM_KEY = "DIM";
    static final String NONE_KEY = "NONE";
    static final String FULL_KEY = "FULL";
    static final String FORMAT_DESC = "(var_)*[FULL|splitVar~[<TypedRange>|DIM~<Long>~<Long>|NONE]]";

    public static String serialise(FileInfo info) {
        StringBuilder sb = new StringBuilder();
        info.vars.forEach(v -> {
            sb.append(v);
            sb.append(SEP);
        });
        if (info.splitVar.isPresent()) {
            sb.append(info.splitVar.get());
            sb.append(TypedRange.SEP);
            int buIndex = sb.length();
            try {
                if (info.trange.isPresent()) {
                    info.trange.get().appendToString(sb);
                } else {
                    writeDRangeOrNone(info, sb);
                }
            } catch (Exception ex) {
                LOG.warn("Got exception while serialising TypedRange of type {}. Writing dimension indices instead. Exception was:\n{}", info.trange.get().getDataType(), ex);
                // rollback
                sb.delete(buIndex, sb.length());
                writeDRangeOrNone(info, sb);
            }
        } else {
            sb.append(FULL_KEY);
        }
        return sb.toString();
    }

    private static void writeDRangeOrNone(FileInfo info, StringBuilder sb) {
        if (info.drange.isPresent()) {
            sb.append(DIM_KEY);
            sb.append(TypedRange.SEP);
            sb.append(info.drange.get().start);
            sb.append(TypedRange.SEP);
            sb.append(info.drange.get().end);
        } else {
            sb.append(NONE_KEY);
        }
    }

    public static FileInfo deserialise(String s) {
        String[] s1 = s.split(TypedRange.SEP, 2); // split on first ~ (the one after splitVar)
        String varsS = s1[0];
        if (s1.length == 1) {
            if (varsS.endsWith(FULL_KEY)) {
                String[] varSplit = varsS.split(SEP);
                List<String> vars = new ArrayList(Arrays.asList(varSplit)); // copy so we can remove next
                vars.remove(vars.size() - 1); // last one is FULL_KEY
                return new FileInfo(vars);
            } else {
                throw new IllegalArgumentException("String \"" + s + "\" has invalid format! Required: " + FORMAT_DESC);
            }
        } else {
            String rangeS = s1[1];
            String[] varSplit = varsS.split(SEP);
            List<String> vars = new ArrayList(Arrays.asList(varSplit)); // copy so we can remove next
            String splitVar = vars.remove(vars.size() - 1); // last one is splitVar
            if (rangeS.equalsIgnoreCase(NONE_KEY)) {
                return new FileInfo(vars, splitVar);
            } else if (rangeS.startsWith(DIM_KEY)) {
                String[] dimSplit = rangeS.split(TypedRange.SEP);
                if (dimSplit.length != 3) {
                    throw new IllegalArgumentException("String \"" + s + "\" has invalid format! Required: " + FORMAT_DESC);
                }
                // index 0 is the DIM_KEY
                long start = Long.parseLong(dimSplit[1]);
                long end = Long.parseLong(dimSplit[2]);
                DimensionRange dr = new DimensionRange(splitVar, start, end, false);
                return new FileInfo(vars, dr);
            } else {
                Optional<TypedRange> rangeO = TypedRange.fromString(rangeS);
                if (rangeO.isPresent()) {
                    return new FileInfo(vars, splitVar, rangeO.get());
                } else {
                    return new FileInfo(vars, splitVar);
                }
            }
        }
    }
}
