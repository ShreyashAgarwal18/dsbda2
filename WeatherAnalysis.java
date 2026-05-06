import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {

    // Mapper: emits all values under a single key "weather"
    public static class WeatherMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text keyOut = new Text("weather");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(" ");

            if (parts.length == 4) {
                double temp = Double.parseDouble(parts[1]);
                double dew = Double.parseDouble(parts[2]);
                double wind = Double.parseDouble(parts[3]);

                context.write(new Text("temp"), new DoubleWritable(temp));
                context.write(new Text("dew"), new DoubleWritable(dew));
                context.write(new Text("wind"), new DoubleWritable(wind));
            }
        }
    }

    // Reducer: calculates average
    public static class WeatherReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: WeatherAnalysis <input_file> <output_folder>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Analysis");

        job.setJarByClass(WeatherAnalysis.class);

        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
================= SIMPLE INSTRUCTIONS =================

1. Save file as:
   WeatherAnalysis.java

2. Create dataset:
   nano sample_weather.txt
   (paste sample data)

3. Compile:
   javac -classpath $(hadoop classpath) -d . WeatherAnalysis.java

4. Create jar:
   jar -cvf weather.jar *

5. Run:
   hadoop jar weather.jar WeatherAnalysis sample_weather.txt output

6. View output:
   cat output/part-r-00000

7. Delete output if needed:
   rm -r output

=====================================================
*/