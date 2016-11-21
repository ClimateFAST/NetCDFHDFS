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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static se.kth.climate.fast.netcdfparquet.Main.LOG;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
  * @deprecated As of 0.3-SNAPSHOT the whole NetCDFParquet API is replaced with NetCDF Alignment.
 */
@Deprecated
public interface DataReader {

    public void prepare(int updatedDimension, Index index) throws IOException, InvalidRangeException;

    public Object read();

    public static class Factory {

        public static DataReader forVar(Variable v) {
            long dimAccum = 1l;
            for (Dimension d : v.getDimensions()) {
                dimAccum *= d.getLength();
            }
            if (dimAccum > Integer.MAX_VALUE) {
                return new RowReader(v);
            } else {
                return new BlockReader(v);
            }
        }
    }

    public static class RowReader implements DataReader {

        private final int updateDim;
        private final Variable v;
        private final int[] size;
        private final int originalUpdateDimSize;
        private final int[] origin;
        private Array currentRow = null;
        private Index idx = null;
        private final String progressS;

        private RowReader(Variable v) {
            this.v = v;
            int dim = -1;
            long dimSize = 0;
            List<Dimension> dims = v.getDimensions();
            for (int i = 0; i < dims.size(); i++) {
                Dimension d = dims.get(i);
                if (d.isUnlimited()) {
                    dim = i;
                    break;
                }
                if (d.getLength() > dimSize) {
                    dimSize = d.getLength();
                    dim = i;
                }
            }
            updateDim = dim;
            size = v.getShape();
            originalUpdateDimSize = size[updateDim];
            size[updateDim] = 1;
            origin = new int[size.length];
            Arrays.fill(origin, 0);
            idx = Index.factory(v.getShape());
            this.progressS = "Progress on " + v.getFullNameEscaped() + ": {}/"+originalUpdateDimSize+" (~{}%)";
        }

        @Override
        public void prepare(int updatedDimension, Index index) throws IOException, InvalidRangeException {
            idx.setDim(updatedDimension, index.getCurrentCounter()[updatedDimension]);
            if (updatedDimension == updateDim) {
                origin[updateDim] = index.getCurrentCounter()[updateDim];
                currentRow = v.read(origin, size);
//                LOG.debug("Read {} at {}/{} for updateDim={}, index={} and got {}/{}", 
//                        new Object[]{v.getFullNameEscaped(), origin, size, updateDim, index, currentRow.getIndex(), currentRow.getShape()});

                idx.setDim(updateDim, 0);
                LOG.debug(progressS, origin[updateDim], (origin[updateDim]*100)/originalUpdateDimSize);
            }
            if (currentRow == null) { // may hapen if the read dimension is not the first
                origin[updateDim] = 0;
                currentRow = v.read(origin, size);
//                LOG.debug("Read initial {} at {}/{} for updateDim={}, index={} and got {}/{}", 
//                        new Object[]{v.getFullNameEscaped(), origin, size, updateDim, index, currentRow.getIndex(), currentRow.getShape()});
            }

        }

        @Override
        public Object read() {
            return currentRow.getObject(idx);
        }

    }

    public static class BlockReader implements DataReader {

        private final Variable v;
        private Array data = null;
        private Index idx = null;

        private BlockReader(Variable v) {
            this.v = v;
        }

        @Override
        public void prepare(int updatedDimension, Index index) throws IOException, InvalidRangeException {
            if (data == null) {
                data = v.read();
            }
            idx = index;
        }

        @Override
        public Object read() {
            return data.getObject(idx);
        }

    }
}
