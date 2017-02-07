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

import fj.data.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solver;
import org.chocosolver.solver.variables.IntVar;
import org.chocosolver.solver.variables.SetVar;
import org.junit.Test;

/**
 *
 * @author lkroll
 */
public class ChocoTest {
    
    public ChocoTest() {
    }
    
    @Test
    public void testSetVars() {
        Model model = new Model("SetVar Test Model");
        SetVar universe = model.setVar("universe", new int[]{1, 2, 3, 4, 5});
        SetVar[] part = model.setVarArray("parts", 5, new int[]{}, new int[]{1, 2, 3, 4, 5});
        model.partition(part, universe).post();
        IntVar[] sizes = Array.array(part).map(set -> set.getCard()).array(IntVar[].class);
//        IntVar[] maxes = model.intVarArray("maxes", part.length, 0, 4);
//        IntVar[] mins = model.intVarArray("mins", part.length, 0, 4);
        int lastElemI = sizes.length - 1;
        for (int i = 0; i < sizes.length; i++) {
            if (i < lastElemI) {
                model.arithm(sizes[i], ">=", sizes[i + 1]).post();
                //model.arithm(mins[i], ">=", maxes[i + 1]).post(); // since both could be 0
            }
//            model.max(part[i], maxes[i], false).post();
//            model.min(part[i], mins[i], false).post();
//            Constraint isZeroSize = model.arithm(sizes[i], "=", 0);
//            model.ifOnlyIf(isZeroSize, model.arithm(maxes[i], "=", 0));
//            model.ifOnlyIf(isZeroSize, model.arithm(mins[i], "=", 0));
        }
        Solver solver = model.getSolver();
        System.out.println("Starting Solver...");
        int numS = 0;
        HashSet<HashSet<TreeSet<Integer>>> solutions = new HashSet<>();
        while (solver.solve()) {
            //System.out.println("Found Solution: " + Arrays.toString(part));
            HashSet<TreeSet<Integer>> solution = Arrays.stream(part).map(set -> {
                TreeSet<Integer> s = new TreeSet<>();
                for (Integer i : set.getValue()) {
                    s.add(i);
                }
                return s;
            }).filter(set -> !set.isEmpty()).collect(Collectors.toCollection(HashSet::new));
            solutions.add(solution);
            numS++;
        }
        // not a real test...just trying to figure out how choco works
        System.out.println("Done (" + numS + " solutions found, "+solutions.size()+" recorded).");
        System.out.println("Solutions:\n" + solutions);
    }
}
