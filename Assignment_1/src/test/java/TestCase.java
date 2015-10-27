/**
 * AIM3 - Advanced Information Management - Methods and Systems
 * Copyright (C) 2015  Christoph Alt
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class TestCase {

    protected static Map<String, Integer> readResults(File output) throws IOException {
        Pattern separator = Pattern.compile("\t");
        Map<String, Integer> results = Maps.newHashMap();
        File fileList[] = getFileList(output);
        for (File outputFile : fileList) {
            if (!outputFile.getName().startsWith("_") && !outputFile.isHidden()) {
                for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
                    String[] tokens = separator.split(line);
                    int count = Integer.parseInt(tokens[1]);
                    results.put(tokens[0], count);
                }
            }
        }
        return results;
    }

    protected static List<String> readLines(File output) throws IOException {
        File fileList[] = getFileList(output);
        ArrayList<String> results = new ArrayList<String>();
        for (File outputFile : fileList) {
            if (!outputFile.getName().startsWith("_") && !outputFile.isHidden()) {
                for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
                    results.add(line);
                }
            }
        }
        return results;
    }

    private static File[] getFileList(File output) {
        File fileList[];
        if (output.isDirectory()) {
            fileList = output.listFiles();
        } else {
            fileList = new File[1];
            fileList[0] = output;
        }
        return fileList;
    }
}