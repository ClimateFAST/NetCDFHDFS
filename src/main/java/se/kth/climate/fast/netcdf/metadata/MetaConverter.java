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
package se.kth.climate.fast.netcdf.metadata;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.common.Constant;
import se.kth.climate.fast.common.DimensionBuilder;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.common.MetadataBuilder;
import se.kth.climate.fast.common.VariableBuilder;
import se.kth.climate.fast.netcdf.MetaInfo;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public abstract class MetaConverter {

    static final Logger LOG = LoggerFactory.getLogger(MetaConverter.class);

    public static Metadata convert(NetcdfFile ncfile, MetaInfo mi) throws IOException {
        MetadataBuilder meta = new MetadataBuilder();
        // GLOBAL
        for (Attribute attr : ncfile.getGlobalAttributes()) {
            meta.addAttribute(attr.getFullNameEscaped(), attr.getStringValue());
        }
        // DIMENSIONS
        for (Dimension d : ncfile.getDimensions()) {
            DimensionBuilder db = new DimensionBuilder();
            fillMetaDimension(d, db);
            meta.addDimension(db);
        }
        // VARIABLES & CONSTANTS
        for (Variable v : ncfile.getVariables()) {
            VariableBuilder vb = new VariableBuilder();
            fillMetaVariable(v, vb);
            if (mi.isConstant(v.getFullName())) {
                Constant c = meta.addConstant(vb);
                fillMetaConstant(v, c);
            } else {
                meta.addVariable(vb);
            }
        }
        return meta.build();
    }

    private static void fillMetaConstant(Variable v, Constant c) throws IOException {

        Array a = v.read();
        switch (v.getDataType()) {
            case DOUBLE: {
                double d = a.getDouble(Index.scalarIndexImmutable);
                c.setReadable(Double.toString(d));
                c.set(d);
            }
            break;

            case FLOAT: {
                float f = a.getFloat(Index.scalarIndexImmutable);
                c.setReadable(Float.toString(f));
                c.set(f);
            }
            break;
            case LONG: {
                long l = a.getLong(Index.scalarIndexImmutable);
                c.setReadable(Long.toString(l));
                c.set(l);
            }
            break;
            case INT: {
                int i = a.getInt(Index.scalarIndexImmutable);
                c.setReadable(Integer.toString(i));
                c.set(i);
            }
            break;
            case BOOLEAN: {
                boolean b = a.getBoolean(Index.scalarIndexImmutable);
                c.setReadable(Boolean.toString(b));
                c.set(b);
            }
            break;
            default:
                LOG.warn("No available primitive {} for constant {}!", v.getDataType(), v);

        }

    }

    private static void fillMetaVariable(Variable v, VariableBuilder vb) {
        vb.setShortName(v.getShortName());
        vb.setStandardName(v.getFullNameEscaped());
        vb.setLongName(v.getFullName());
        vb.setUnit(v.getUnitsString());
        vb.setType(v.getDataType());
        for (Attribute attr : v.getAttributes()) {
            if (se.kth.climate.fast.common.Variable.includeAttribute(attr.getFullNameEscaped())) {
                vb.addAttribute(attr.getFullNameEscaped(), attr.getStringValue());
            }
        }
    }

    private static void fillMetaDimension(Dimension d, DimensionBuilder db) {
        db.setName(d.getFullNameEscaped());
        db.setUnlimited(d.isUnlimited());
        db.setSize(d.getLength());
        if (d.isVariableLength()) {
            LOG.warn("Dimension {} is declared variable length. There is no support for this feature, yet!", d);
        }
    }
}
