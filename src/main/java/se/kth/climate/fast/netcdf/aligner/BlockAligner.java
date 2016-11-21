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

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import fj.data.Array;
import fj.data.Either;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solver;
import org.chocosolver.solver.variables.IntVar;
import org.chocosolver.solver.variables.SetVar;
import org.javatuples.Pair;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.climate.fast.netcdf.MetaInfo;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;

/**
 *
 * @author lkroll
 */
public class BlockAligner {
    
    static final Logger LOG = LoggerFactory.getLogger(BlockAligner.class);

    public final long blockSize;
    public final MetaInfo metaInfo;
    public final AssignmentQualityMeasure measure;

    public BlockAligner(long blockSize, MetaInfo mInfo, AssignmentQualityMeasure measure) {
        this.blockSize = blockSize;
        this.metaInfo = mInfo;
        this.measure = measure;
    }

    public VariableAlignment align() {
        List<List<VariableAssignment>> assignments = enumeratePossibleAssignments();
        List<VariableAlignment> fits = assignments.stream()
                .map(this::fit).collect(Collectors.toList());
        LOG.debug("Got fits:\n{}", fits);
        List<Pair<VariableAlignment, Double>> measured = fits.stream()
                .map(va -> Pair.with(va, measure.score(va, metaInfo))).collect(Collectors.toList());
        LOG.debug("Ratings (unsorted):\n{}", measured);
        measured.sort((Pair<VariableAlignment, Double> p1, Pair<VariableAlignment, Double> p2) -> {
            double diff = p2.getValue1() - p1.getValue1();
            return (int) Math.signum(diff);
        });
        LOG.debug("Ratings:\n{}", measured);
        return measured.get(0).getValue0(); // return the alignment with the best rating
    }

    private VariableAlignment fit(List<VariableAssignment> vas) {
        BlockFitter bf = new BlockFitter(vas, metaInfo, blockSize);
        return bf.fit();
    }

    private List<List<VariableAssignment>> enumeratePossibleAssignments() {
        final DirectedPseudograph<Either<DimName, VarName>, DefaultEdge> relG = constructGraph(metaInfo);
        final FloydWarshallShortestPaths<Either<DimName, VarName>, DefaultEdge> fwsp = new FloydWarshallShortestPaths(relG);
        final List<VariableGroup> varGroups = metaInfo.ncfile.getVariables().stream()
                .filter((v) -> {
                    Either<DimName, VarName> vn = EitherName.of(v);
                    if (relG.containsVertex(vn)) {
                        return relG.incomingEdgesOf(vn).isEmpty();
                    }
                    return false;
                })
                .map((v) -> Pair.with(v, fwsp.getShortestPaths(EitherName.of(v))))
                .map((pvsps) -> {
                    List<Pair<VarName, Integer>> pathLengths = pvsps.getValue1().stream()
                    .filter(path -> path.getEndVertex().isRight())
                    .map(path -> Pair.with(path.getEndVertex().right().value(), path.getLength()))
                    .collect(Collectors.toList());
                    return VariableGroup.of(pvsps.getValue0(), pathLengths);
                })
                .collect(Collectors.toList());
        final HashSet<HashSet<TreeSet<Integer>>> partitions = enumeratePartitions(varGroups.size());
        return partitions.stream().map(p -> part2VA(p, varGroups)).collect(Collectors.toList());
    }

    private List<VariableAssignment> part2VA(HashSet<TreeSet<Integer>> partitioning, List<VariableGroup> varGroups) {
        return partitioning.stream().map((partition) -> {
            List<VariableGroup> partVGs = partition.stream().map(i -> varGroups.get(i)).collect(Collectors.toList());
            final Set<String> infVariables = new HashSet<>();
            final Set<String> dimensionVariables = new HashSet<>();
            final Set<String> boundsVariables = new HashSet<>();
            final Set<String> otherVariables = new HashSet<>();
            partVGs.forEach((vg) -> {
                if (vg.var.isUnlimited()) {
                    infVariables.add(vg.var.getFullNameEscaped());
                } else {
                    otherVariables.add(vg.var.getFullNameEscaped());
                }
                vg.paths.keySet().forEach((varName) -> {
                    if (metaInfo.isBounds(varName)) {
                        boundsVariables.add(varName);
                    } else if (metaInfo.isDescription(varName)) {
                        dimensionVariables.add(varName);
                    } else {
                        otherVariables.add(varName);
                    }
                });
            });
            return VariableAssignment.assign(infVariables, dimensionVariables, boundsVariables, otherVariables, metaInfo.getConstants());
        }).collect(Collectors.toList());
    }

    private HashSet<HashSet<TreeSet<Integer>>> enumeratePartitions(int size) {
        int[] range = IntStream.range(0, size).toArray();
        Model model = new Model("Variable Partitioning Model");
        SetVar universe = model.setVar("universe", range);
        SetVar[] part = model.setVarArray("parts", size, new int[]{}, range);
        model.partition(part, universe).post();
        IntVar[] sizes = Array.array(part).map(set -> set.getCard()).array(IntVar[].class);
        int lastElemI = sizes.length - 1;
        for (int i = 0; i < lastElemI; i++) {
            model.arithm(sizes[i], ">=", sizes[i + 1]).post();
        }
        Solver solver = model.getSolver();
        HashSet<HashSet<TreeSet<Integer>>> solutions = new HashSet<>();
        while (solver.solve()) {
            HashSet<TreeSet<Integer>> solution = Arrays.stream(part).map(set -> {
                TreeSet<Integer> s = new TreeSet<>();
                for (Integer i : set.getValue()) {
                    s.add(i);
                }
                return s;
            }).filter(set -> !set.isEmpty()).collect(Collectors.toCollection(HashSet::new));
            solutions.add(solution);
        }
        return solutions;
    }

    private DirectedPseudograph<Either<DimName, VarName>, DefaultEdge> constructGraph(MetaInfo metaInfo) {
        DirectedPseudograph<Either<DimName, VarName>, DefaultEdge> g = new DirectedPseudograph<>(DefaultEdge.class);
        for (Dimension d : metaInfo.ncfile.getDimensions()) {
            g.addVertex(EitherName.of(d));
        }
        for (Variable v : metaInfo.ncfile.getVariables()) {
            if (metaInfo.isConstant(v.getFullNameEscaped())) {
                continue; // skip constants as they are unconnected nodes that should rather be fully connected
                // instead add them back in later
            }
            g.addVertex(EitherName.of(v));
            for (Dimension d : v.getDimensions()) {
                g.addEdge(EitherName.of(v), EitherName.of(d));
                if (metaInfo.isDescription(v, d) || (metaInfo.isBounds(v))) {
                    g.addEdge(EitherName.of(d), EitherName.of(v)); // add back edge
                }
            }
        }
        LOG.debug("Variable Graph:\n{}", g);
        return g;
    }

    private static class DimName implements Comparable<DimName> {

        final String name;

        private DimName(String name) {
            this.name = name;
        }

        static DimName of(Dimension d) {
            return new DimName(d.getFullNameEscaped());
        }

        @Override
        public int compareTo(DimName that) {
            return ComparisonChain.start()
                    .compare(this.name, that.name)
                    .result();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof DimName) {
                DimName that = (DimName) o;
                return this.compareTo(that) == 0;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 29 * hash + Objects.hashCode(this.name);
            return hash;
        }

        @Override
        public String toString() {
            return name;
        }

    }

    private static class VarName implements Comparable<VarName> {

        final String name;

        private VarName(String name) {
            this.name = name;
        }

        static VarName of(Variable v) {
            return new VarName(v.getFullNameEscaped());
        }

        @Override
        public int compareTo(VarName that) {
            return ComparisonChain.start()
                    .compare(this.name, that.name)
                    .result();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof VarName) {
                VarName that = (VarName) o;
                return this.compareTo(that) == 0;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 53 * hash + Objects.hashCode(this.name);
            return hash;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static abstract class EitherName {

        static Either<DimName, VarName> of(Variable v) {
            return Either.right(VarName.of(v));
        }

        static Either<DimName, VarName> of(Dimension d) {
            return Either.left(DimName.of(d));
        }
    }

    private static class VariableGroup {

        final Variable var;
        final ImmutableMap<String, Integer> paths;

        private VariableGroup(Variable var, ImmutableMap<String, Integer> paths) {
            this.var = var;
            this.paths = paths;
        }

        static VariableGroup of(Variable var, List<Pair<VarName, Integer>> paths) {
            ImmutableMap.Builder<String, Integer> imb = ImmutableMap.builder();
            paths.forEach(path -> imb.put(path.getValue0().name, path.getValue1()));
            return new VariableGroup(var, imb.build());
        }
    }
}
