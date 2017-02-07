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
import java.util.List;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class FileInfo {

    public final List<String> vars;
    public final Optional<String> splitVar;
    public final Optional<TypedRange> trange;
    public final Optional<DimensionRange> drange;

    public FileInfo(List<String> vars, DimensionRange drange) {
        this.vars = vars;
        this.splitVar = Optional.of(drange.name);
        this.trange = Optional.absent();
        this.drange = Optional.of(drange);
    }

    public FileInfo(List<String> vars, TypedRange trange, DimensionRange drange) {
        this.vars = vars;
        this.splitVar = Optional.of(drange.name);
        this.trange = Optional.of(trange);
        this.drange = Optional.of(drange);
    }

    public FileInfo(List<String> vars, String splitVar, TypedRange trange) {
        this.vars = vars;
        this.splitVar = Optional.of(splitVar);
        this.trange = Optional.of(trange);
        this.drange = Optional.absent();
    }

    public FileInfo(List<String> vars, String splitVar) {
        this.vars = vars;
        this.splitVar = Optional.of(splitVar);
        this.trange = Optional.absent();
        this.drange = Optional.absent();
    }

    public FileInfo(List<String> vars) {
        this.vars = vars;
        this.splitVar = Optional.absent();
        this.trange = Optional.absent();
        this.drange = Optional.absent();
    }
}
