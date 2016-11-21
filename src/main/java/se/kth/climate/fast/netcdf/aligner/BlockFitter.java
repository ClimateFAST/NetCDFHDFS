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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.netcdf.DataDescriptor;
import se.kth.climate.fast.netcdf.DimensionRange;
import se.kth.climate.fast.netcdf.MetaInfo;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class BlockFitter {
    
    static final Logger LOG = LoggerFactory.getLogger(BlockFitter.class);

    private final List<VariableAssignment> vas;
    private final MetaInfo mInfo;
    private final long blockSize;
    public static final long ESTIMATION_MARGIN = 100; //100bytes

    public BlockFitter(List<VariableAssignment> vas, MetaInfo mi, long blockSize) {
        this.vas = vas;
        this.mInfo = mi;
        this.blockSize = blockSize;
    }

    public VariableAlignment fit() {
        LOG.debug("Starting to fit: " + vas);
        List<Pair<VariableAssignment, DataDescriptor>> fullDD = vas.stream().map(this::va2ddFull).collect(Collectors.toList());
        List<Triplet<VariableAssignment, DataDescriptor, Long>> fullSizes = fullDD.stream()
                .map(pdd -> Triplet.with(pdd.getValue0(), pdd.getValue1(), pdd.getValue1().estimateSize()))
                .collect(Collectors.toList());
        LOG.debug("Full Descriptors:\n" + fullSizes);
        List<Pair<VariableAssignment, VariableFit>> fits;
        if (fullSizes.stream().allMatch(tsize -> tsize.getValue2() < blockLimit())) {
            LOG.debug("Assignment fits without splitting: " + vas);
            fits = fullDD.stream().map(pdd
                    -> Pair.with(pdd.getValue0(), VariableFit.fromDataDescriptors(ImmutableList.of(pdd.getValue1())))
            ).collect(Collectors.toList());
        } else {
            LOG.debug("Splitting assignment to fit: " + vas);
            // fit assignments separately
            fits = fullSizes.stream()
                    .map(this::fitWithDD).collect(Collectors.toList());
        }
        VariableAlignment va = VariableAlignment.of(fits);
        LOG.debug("Finished fitting: " + vas + " with " + va);
        return va;
    }

    private Pair<VariableAssignment, VariableFit> fitWithDD(Triplet<VariableAssignment, DataDescriptor, Long> pvadds) {
        final VariableAssignment va = pvadds.getValue0();
        final DataDescriptor initialDD = pvadds.getValue1();
        final long initialSize = pvadds.getValue2();
        double blockRatio = (double) blockLimit() / (double) initialSize;
        LOG.debug("block ratio is {}", blockRatio);
        List<String> rdims = rankDimensions(initialDD);

        if (rdims.isEmpty()) {
            throw new FittingException("Ranks are empty!", va, initialDD);
        }
        LOG.debug("Ranked dimensions: {}", rdims);
        
        int curDim = 0;
        DimensionRange dr1 = initialDD.dims.get(rdims.get(curDim));
        while (dr1.getSize() <= 1) {
            curDim++;
            if (rdims.size() > curDim) {
                dr1 = initialDD.dims.get(rdims.get(curDim));
            } else {
                throw new FittingException("No non-constant dimensions to split over!", va, initialDD);
            }
        }
        long perBlockSize = (long) Math.floor(blockRatio * ((double) dr1.getSize()));
        LOG.debug("Splitting over {} with {} slices per block", dr1, perBlockSize);
        if (perBlockSize > 0) { // this should fit with at least a single slice per file
            // generate sub ranges
            List<DimensionRange> subRanges = new LinkedList<>();
            long offset = dr1.start;
            while (offset < dr1.end) { // NOTE: If there's an off by one issue, it's probably here^^
                long endset = Math.min(offset + perBlockSize, dr1.end);
                subRanges.add(new DimensionRange(dr1.name, offset, endset, dr1.inf));
                offset = endset + 1;
            }
            LOG.debug("Generated subranges:\n{}", subRanges);
            final Map<String, DimensionRange> otherDRs = new HashMap<>(initialDD.dims);
            otherDRs.remove(dr1.name);
            List<DataDescriptor> newDDs = subRanges.stream().map(splitdr -> {
                ImmutableMap.Builder<String, DimensionRange> drsB = ImmutableMap.builder();
                drsB.putAll(otherDRs);
                drsB.put(splitdr.name, splitdr);
                return new DataDescriptor(initialDD.metaInfo, initialDD.vars, drsB.build(), Optional.of(splitdr.name));
            }).collect(Collectors.toList());
            LOG.debug("Generated new data descritors:\n{}", newDDs);
            long firstSize = newDDs.get(0).estimateSize();
            if (firstSize < blockLimit()) {
                return Pair.with(va, VariableFit.fromDataDescriptors(ImmutableList.copyOf(newDDs)));
            } else {
                // TODO write a more flexible fitter, that tries decrements of ranges until it fits (if possible)
                throw new FittingException("It should have fit, but it decided not to. Estimated size was "
                        + firstSize + "bytes of limit " + blockLimit() + "bytes."
                        + " Complain to the devs to write a better fitting algorithm.", va, newDDs.get(0));
            }
            // TODO write a tigher fitter, that tries increments of ranges to waste less space where dimensions impact multiple variables
        } else { // this won't fit...could split along a different dimension but for now just throw an exception
            throw new FittingException("Splitting over multiple dimensions not yet implemented!", va, initialDD);
        }
    }

    private Pair<VariableAssignment, DataDescriptor> va2ddFull(VariableAssignment va) {
        final Set<String> vars = new HashSet<>();
        final Map<String, DimensionRange> dims = new HashMap<>();
        Consumer<String> addRange = (String varName) -> {
            vars.add(varName);
            Variable v = mInfo.getVariable(varName);
            for (Dimension dim : v.getDimensions()) {
                DimensionRange dr = new DimensionRange(dim.getFullNameEscaped(), 0, dim.getLength(), dim.isUnlimited());
                dims.putIfAbsent(dr.name, dr);
            }
        };
        va.infVariables.forEach(addRange);
        va.boundsVariables.forEach(addRange);
        va.dimensionVariables.forEach(addRange);
        va.otherVariables.forEach(addRange);
        va.constants.forEach(addRange);
        return Pair.with(va, new DataDescriptor(mInfo, vars, dims, Optional.absent()));
    }

    private long blockLimit() {
        return blockSize - ESTIMATION_MARGIN;
    }

    private List<String> rankDimensions(final DataDescriptor initialDD) {
        Comparator<String> dimComp = (String dim1, String dim2) -> {
            if (dim1.equals(dim2)) {
                return 0;
            }
            DimensionRange dr1 = initialDD.dims.get(dim1);
            DimensionRange dr2 = initialDD.dims.get(dim2);
            if (dr1.inf || !dr2.inf) {
                return 1;
            }
            if (dr2.inf || !dr1.inf) {
                return -1;
            }
            long diff = dr1.getSize() - dr2.getSize();
            if (diff != 0) {
                return Long.signum(diff);
            } else {
                return dim1.compareTo(dim2); // if everything else is equal just compare the strings
            }
        };
        List<String> sortedDims = new ArrayList(initialDD.dims.keySet());
        sortedDims.sort(Ordering.from(dimComp).reversed());
        return sortedDims;
    }

}
