/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment2;


import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Classification {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
        DataSource<String> sumInput = env.readTextFile(Config.pathToSums());

        DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());
        DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

        DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

        DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData.map(new Classifier())
                .withBroadcastSet(conditionals, "conditionals")
                .withBroadcastSet(sums, "sums");

        classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

        @Override
        public Tuple3<String, String, Long> map(String s) throws Exception {
            String[] elements = s.split("\t");
            return new Tuple3<String, String, Long>(elements[0], elements[1], Long.parseLong(elements[2]));
        }
    }

    public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(String s) throws Exception {
            String[] elements = s.split("\t");
            return new Tuple2<String, Long>(elements[0], Long.parseLong(elements[1]));
        }
    }


    public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>> {

        private final Map<String, Map<String, Long>> wordCounts = Maps.newHashMap();
        private final Map<String, Long> wordSums = Maps.newHashMap();
        private final double smoothing = (double) Config.getSmoothingParameter();
        private int distinctTerms = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            List<Tuple3<String, String, Long>> conditionals = getRuntimeContext().getBroadcastVariable("conditionals");
            for (Tuple3<String, String, Long> tup3 : conditionals) {
                if (wordCounts.containsKey(tup3.f0)) {
                    wordCounts.get(tup3.f0).put(tup3.f1, tup3.f2);
                } else {
                    Map<String, Long> temp = Maps.newHashMap();
                    temp.put(tup3.f1, tup3.f2);
                    wordCounts.put(tup3.f0, temp);
                }
            }
            List<Tuple2<String, Long>> sums = getRuntimeContext().getBroadcastVariable("sums");
            for (Tuple2<String, Long> tup2 : sums) {
                wordSums.put(tup2.f0, (long) wordCounts.get(tup2.f0).size());
            }
            Map<String, Long> temp = Maps.newHashMap();
            wordCounts.values().forEach(temp::putAll);
            distinctTerms = temp.size();
        }

        @Override
        public Tuple3<String, String, Double> map(String line) throws Exception {

            String[] tokens = line.split("\t");
            String label = tokens[0];
            String[] terms = tokens[1].split(",");

            double maxProbability = Double.NEGATIVE_INFINITY;
            String predictionLabel = "";

            for (String category : wordSums.keySet()) {
                double logProbability = calculateLogProbability(category, terms);
                if (logProbability > maxProbability) {
                    maxProbability = logProbability;
                    predictionLabel = category;
                }
            }

            // label is here the actual label
            return new Tuple3<String, String, Double>(label, predictionLabel, maxProbability);
        }
        int counter = 0;
        private double calculateLogProbability(String category, String[] terms) {
            System.out.println(counter++);
            double tel = Arrays.stream(terms).mapToDouble(term -> {
                double temp = 0;
                if (wordCounts.get(category).containsKey(term)) {
                    temp = Math.log(wordCounts.get(category).get(term) + this.smoothing);
                } else {
                    temp = Math.log(this.smoothing);
                }
                return temp;
            }).sum();

            return tel - terms.length * Math.log(wordSums.get(category) + this.smoothing * distinctTerms);
        }

    }

}
