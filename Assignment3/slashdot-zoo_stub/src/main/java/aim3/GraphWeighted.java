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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


public class GraphWeighted extends Graph {

    public GraphWeighted(ExecutionEnvironment env, String input, String output,
                 String jobDescription) {
        super(env, input, output, jobDescription);
    }

    public void computeWeightedDistribution(FlatMapFunction<Tuple3<Long, Long, Boolean>,
            Tuple2<Long, Long>> emitEdges) throws Exception {
    	// TODO Implement Me
    	
        // Read the input matrix with source id, target id, connection (friend == +1, foe == -1)

        // Count the number of distinct vertices in graph

        // Compute the degree for each vertex (vertex ID, degree)

        // Compute the degrees (degree, count)

        // The number of vertices without degree must be computed manually

        // Compute the degree distribution (degree, probability)
        DataSet<Tuple2<Long, Double>> degreeDistribution = null;

        degreeDistribution.writeAsCsv(output, FileSystem.WriteMode.OVERWRITE);

        env.execute(jobDescription);
    }
    
}
