package de.tuberlin.dima.aim3.assignment1;


import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageTemperaturePerMonth extends HadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Map<String, String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

        Job averageTemperature = prepareJob(inputPath, outputPath, TextInputFormat.class,
                AverageTemperatureMapper.class, Text.class, IntWritable.class,
                AverageTemperatureReducer.class, Text.class, DoubleWritable.class,
                TextOutputFormat.class);

        averageTemperature.getConfiguration().set("minimumQuality", String.valueOf(minimumQuality));

        averageTemperature.waitForCompletion(true);

        return 0;
    }


    static class AverageTemperatureMapper extends Mapper<Object, Text, Text, IntWritable> {


        @Override
        protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            double minimumQuality = Double.parseDouble(conf.get("minimumQuality"));

            String l = line.toString();
            String[] ls = l.split("\t");
            if (minimumQuality <= Double.parseDouble(ls[ls.length - 1])) {
                String K = ls[0] + "\t" + ls[1];
                ctx.write(new Text(K), new IntWritable(Integer.parseInt(ls[2])));
            }

        }

    }

    static class AverageTemperatureReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            int length = 0;
            for (IntWritable value : values) {
                sum += value.get();
                length++;
            }
            double average = (double) sum / length;
            ctx.write(new Text(key), new DoubleWritable(average));
        }
    }
}