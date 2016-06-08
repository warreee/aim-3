package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class FilteringWordCount extends HadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Map<String, String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
                Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);

        wordCount.waitForCompletion(true);

        return 0;
    }

    static class FilteringWordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private ArrayList<String> filterList = new ArrayList<>();

        /**
         * Method to add words that should be filtered out.
         *
         * @param fl : the list with filtered words
         */
        public void addWordsToFilter(List<String> fl) {
            this.filterList.addAll(fl);
        }

        public List getFilterList() {
            return this.filterList;
        }


        @Override
        protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
            String[] filterList = {"to", "and", "in", "the"};
            addWordsToFilter(Arrays.asList(filterList));
            Pattern.compile(" ").splitAsStream(line.toString().replace(",", "")).map(String::toLowerCase)
                    .filter(l -> !getFilterList().contains(l)).collect(groupingBy(Function.identity(), counting()))
                    .forEach((word, count) -> {
                        try {
                            ctx.write(new Text(word), new IntWritable(count.intValue()));
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });


        }

    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {

            ctx.write(key, new IntWritable(StreamSupport.stream(values.spliterator(), false).mapToInt(IntWritable::get).sum()));
        }

    }

}