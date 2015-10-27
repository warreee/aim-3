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

import de.tu_berlin.dima.aim3.Config;
import de.tu_berlin.dima.aim3.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class WordCountTestCase extends TestCase {
    @Test
    public void testWordCountComputation() throws Exception {
        System.out.println("Test wordcount computation");

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapreduce.framework.name", "local");
        Path input = new Path(Config.inputPathWordCountTest());
        Path output = new Path(Config.outputPath());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output
        WordCount driver = new WordCount();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] { input.toString(), output.toString() });
        assert(exitCode == 0);

        // read csv file with results and compare
        Map<String, Integer> results = readResults(new File(Config.outputPath()));

        // verify results
        assertEquals(new Integer(2), results.get("the"));
        assertEquals(new Integer(1), results.get("now"));
        assertEquals(new Integer(1), results.get("look"));
        assertEquals(new Integer(1), results.get("viewest"));
        assertFalse(results.containsKey(""));
        assertFalse(results.containsKey(" "));
    }
}
