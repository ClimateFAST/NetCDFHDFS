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
import ucar.nc2.Attribute;
import ucar.nc2.Variable;
import ucar.nc2.time.Calendar;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.time.CalendarDateUnit;
import ucar.nc2.time.CalendarPeriod;

/**
 *
 * @author lkroll
 * @deprecated As of 0.3-SNAPSHOT the whole NetCDFParquet API is replaced with
 * NetCDF Alignment.
 */
@Deprecated
public class DateOffsetter implements Offsetter {

    private final CalendarDateUnit unit;
    private CalendarDateUnit cur;
    private double offset;

    public DateOffsetter(Variable v) {
        Attribute calAttr = v.findAttribute("calendar");
        Calendar cal;
        if (calAttr == null) {
            LOG.info("No calendar attribute for variable {}. Using gregorian.", v.getFullNameEscaped());
            cal = Calendar.gregorian;
        } else {
            cal = Calendar.get(calAttr.getStringValue());
            if (cal == null) {
                LOG.warn("Couldn't find calendar for {}. Falling back to gregorian.", calAttr);
                cal = Calendar.gregorian;
            } else {
                LOG.debug("Using calendar {} for variable {}", cal, v.getFullNameEscaped());
            }
        }
        Attribute unitAttr = v.findAttribute("units");
        if (unitAttr == null) {
            LOG.info("No units attribute for variable {}. Using unix ts.", v.getFullNameEscaped());
            unit = CalendarDateUnit.of(cal, CalendarPeriod.Field.Millisec, CalendarDate.of(0));
        } else {
            unit = CalendarDateUnit.withCalendar(cal, unitAttr.getStringValue());
            LOG.debug("Using unit {} for variable {}", unit, v.getFullNameEscaped());
        }
    }

    public void update(Variable v) {
        Attribute calAttr = v.findAttribute("calendar");
        Calendar cal;
        if (calAttr == null) {
            LOG.info("No calendar attribute for variable {}. Using gregorian.", v.getFullNameEscaped());
            cal = Calendar.gregorian;
        } else {
            cal = Calendar.get(calAttr.getStringValue());
            if (cal == null) {
                LOG.warn("Couldn't find calendar for {}. Falling back to gregorian.", calAttr);
                cal = Calendar.gregorian;
            } else {
                LOG.debug("Using calendar {} for variable {}", cal, v.getFullNameEscaped());
            }
        }
        Attribute unitAttr = v.findAttribute("units");
        if (unitAttr == null) {
            LOG.info("No units attribute for variable {}. Using unix ts.", v.getFullNameEscaped());
            cur = CalendarDateUnit.of(cal, CalendarPeriod.Field.Millisec, CalendarDate.of(0));
        } else {
            cur = CalendarDateUnit.withCalendar(cal, unitAttr.getStringValue());
            LOG.debug("Using unit {} for variable {}", unit, v.getFullNameEscaped());
        }

        CalendarDate cdBase = unit.makeCalendarDate(0);
        CalendarDate dbCur = cur.makeCalendarDate(0);
        offset = unit.getTimeUnit().getOffset(cdBase, dbCur);
    }

    @Override
    public Object offset(Object original) {
        Double orig = (Double) original;
        return orig + offset;
    }
}
