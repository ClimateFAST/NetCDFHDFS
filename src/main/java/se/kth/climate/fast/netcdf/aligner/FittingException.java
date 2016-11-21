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

import se.kth.climate.fast.netcdf.DataDescriptor;

/**
 *
 * @author lkroll
 */
public class FittingException extends RuntimeException {

    private static final long serialVersionUID = -4057874211960123354L;

    private final String fullMessage;

    public FittingException(String msg, VariableAssignment va, DataDescriptor dd) {
        super(msg);
        StringBuilder sb = new StringBuilder();
        sb.append("FittingException(\n");
        sb.append("   assignment: ");
        sb.append(va);
        sb.append("\n   data descriptor: ");
        sb.append(dd);
        sb.append("\n   msg: \"");
        sb.append(msg);
        sb.append("\")");
        fullMessage = sb.toString();
    }

    @Override
    public String toString() {
        return fullMessage;
    }

}
