/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aim3;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class SlashdotZooFoeOutDegree {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String input = Config.pathToSlashdotZoo();
        String output = Config.outputPath();

        GraphWeighted directedGraph = new GraphWeighted(env, input, output,
                "SlashdotZoo - Friend Out-Degree Distribution");


        directedGraph.computeWeightedDistribution(new FoeOutDegree());
    }

    public static class FoeOutDegree
            implements FlatMapFunction<Tuple3<Long, Long, Boolean>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

        public void flatMap(Tuple3<Long, Long, Boolean> edge, Collector<Tuple2<Long, Long>> out)
                throws Exception {
            // if edge is a foe relation
            if (!edge.f2) {
                out.collect(new Tuple2<Long, Long>(edge.f0, 1L));
            }
        }
    }
}

