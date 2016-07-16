/*
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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * This program processes web logs and relational data.
 * It implements the following relational query:
 * <p>
 * <pre>{@code
 * SELECT
 *       r.pageRank,
 * FROM rankings r JOIN
 *      (
 *      SELECT destURL, count(destURL) AS pageHits
 *      FROM Visits v
 *      WHERE
 *          v.languageCode <> ['DE']
 *      GROUP BY destURL
 *      ORDER BY pageHits DESC
 *      LIMIT 1
 *      )
 *      ON destURL = r.pageURL
 * }</pre>
 * <p>
 * The query answers the following question:
 * What is the rank for the most visited website based on IPs that do not originate in Germany?
 * </p>
 * <p>
 * Input files are plain text CSV files using the pipe character ('|') as field separator.
 * The queries give you an idea on how the data looks like.
 * <pre>{@code
 * CREATE TABLE Documents (
 *                url VARCHAR(100) PRIMARY KEY,
 *                contents TEXT );
 *
 * CREATE TABLE Rankings (
 *                pageRank INT,
 *                pageURL VARCHAR(100) PRIMARY KEY,
 *                avgDuration INT );
 *
 * CREATE TABLE Visits (
 *                destURL VARCHAR(100),
 *                visitDate DATE,
 *                sourceIP VARCHAR(16),
 *                languageCode VARCHAR(6),
 * }</pre>
 * <p>
 * <p>
 * <p>
 * Usage: <code>WebLogAnalysis &lt;documents path&gt; &lt;ranks path&gt; &lt;visits path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WebLogData}.
 * <p>
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> tuple data types
 * <li> projection and join projection
 * <li> the CoGroup transformation for an anti-join
 * </ul>
 */
@SuppressWarnings("serial")
public class WebLogAnalysis2 {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataSet<Tuple2<String, String>> documents = getDocumentsDataSet(env);
        DataSet<Tuple3<Integer, String, Integer>> ranks = getRanksDataSet(env);
        DataSet<Tuple4<String, String, String, String>> visits = getVisitsDataSet(env);


        // Filter visits by visit country
        DataSet<Tuple1<String>> filterVisits = visits
                .filter(new FilterVisitsByCountry())
                .project(0);

        DataSet<Tuple2<String, Integer>> visitsCount =
                filterVisits.map(new Counter()).groupBy(0).aggregate(Aggregations.SUM, 1);

        DataSet<Tuple1<Integer>> result = visitsCount.aggregate(Aggregations.MAX,1).project(1);



        // emit result
        if (fileOutput) {
            result.writeAsCsv(outputPath, "\n", "|");
            // execute program
            env.execute("WebLogAnalysis Example");
        } else {
            result.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************





    public static class Counter implements MapFunction<Tuple1<String>, Tuple2<String, Integer>> {

        public Tuple2<String, Integer> map(Tuple1<String> visit) throws Exception {

            return new Tuple2<String, Integer>(visit.f0, 1);

        }
    }


    /**
     * MapFunction that filters for documents that contain a certain set of
     * keywords.
     */
    public static class FilterDocByKeyWords implements FilterFunction<Tuple2<String, String>> {

        private static final String[] KEYWORDS = {" editors ", " oscillations "};

        /**
         * Filters for documents that contain all of the given keywords and projects the records on the URL field.
         * <p>
         * Output Format:
         * 0: URL
         * 1: DOCUMENT_TEXT
         */

        public boolean filter(Tuple2<String, String> value) throws Exception {
            // FILTER
            // Only collect the document if all keywords are contained
            String docText = value.f1;
            for (String kw : KEYWORDS) {
                if (!docText.contains(kw)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * MapFunction that filters for records where the rank exceeds a certain threshold.
     */
    public static class FilterByRank implements FilterFunction<Tuple3<Integer, String, Integer>> {

        private static final int RANKFILTER = 40;

        /**
         * Filters for records of the rank relation where the rank is greater
         * than the given threshold.
         * <p>
         * Output Format:
         * 0: RANK
         * 1: URL
         * 2: AVG_DURATION
         */

        public boolean filter(Tuple3<Integer, String, Integer> value) throws Exception {
            return (value.f0 > RANKFILTER);
        }
    }

    /**
     * MapFunction that filters for records of the visits relation where the year
     * (from the date string) is equal to a certain value.
     */
    public static class FilterVisitsByDate implements FilterFunction<Tuple4<String, String, String, String>> {

        private static final int YEARFILTER = 2007;

        /**
         * Filters for records of the visits relation where the year of visit is equal to a
         * specified value. The URL of all visit records passing the filter is emitted.
         * <p>
         * Output Format:
         * 0: URL
         * 1: DATE
         * 2: SOURCE_IP
         * 3: LANGUAGE_CODE
         */

        public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
            // Parse date string with the format YYYY-MM-DD and extract the year
            String dateString = value.f1;
            int year = Integer.parseInt(dateString.substring(0, 4));
            return (year == YEARFILTER);
        }
    }

    public static class FilterVisitsByCountry implements FilterFunction<Tuple4<String, String, String, String>> {

        private static final String COUNTRYFILTER = "DE";

        public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
            String country = value.f3;
            return !country.equals(COUNTRYFILTER);
        }

    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String documentsPath;
    private static String ranksPath;
    private static String visitsPath;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            fileOutput = true;
            if (args.length == 4) {
                documentsPath = args[0];
                ranksPath = args[1];
                visitsPath = args[2];
                outputPath = args[3];
            } else {
                System.err.println("Usage: WebLogAnalysis <documents path> <ranks path> <visits path> <result path>");
                return false;
            }
        } else {
            System.out.println("Executing WebLog Analysis example with built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  We provide a data generator to create synthetic input files for this program.");
            System.out.println("  Usage: WebLogAnalysis <documents path> <ranks path> <visits path> <result path>");
        }
        return true;
    }

    private static DataSet<Tuple2<String, String>> getDocumentsDataSet(ExecutionEnvironment env) {
        // Create DataSet for documents relation (URL, Doc-Text)
        if (fileOutput) {
            return env.readCsvFile(documentsPath)
                    .fieldDelimiter("|")
                    .types(String.class, String.class);
        } else {
            return WebLogData.getDocumentDataSet(env);
        }
    }

    private static DataSet<Tuple3<Integer, String, Integer>> getRanksDataSet(ExecutionEnvironment env) {
        // Create DataSet for ranks relation (Rank, URL, Avg-Visit-Duration)
        if (fileOutput) {
            return env.readCsvFile(ranksPath)
                    .fieldDelimiter("|")
                    .types(Integer.class, String.class, Integer.class);
        } else {
            return WebLogData.getRankDataSet(env);
        }
    }

    private static DataSet<Tuple4<String, String, String, String>> getVisitsDataSet(ExecutionEnvironment env) {
        // Create DataSet for visits relation (URL, Date, IP, LanguageCode)
        if (fileOutput) {
            return env.readCsvFile(visitsPath)
                    .fieldDelimiter("|")
                    .types(String.class, String.class, String.class, String.class);
        } else {
            return WebLogData.getVisitDataSet(env);
        }
    }

}