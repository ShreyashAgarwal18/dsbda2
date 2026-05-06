import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogProcessor {

    // Mapper: extracts log level and emits (level, 1)
    public static class LogMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text logLevel = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Simple parsing: assume log level exists in line
            // Example: "2024-01-01 ERROR Something failed"
            if (line.contains("ERROR")) {
                logLevel.set("ERROR");
                context.write(logLevel, one);
            } else if (line.contains("INFO")) {
                logLevel.set("INFO");
                context.write(logLevel, one);
            } else if (line.contains("WARN")) {
                logLevel.set("WARN");
                context.write(logLevel, one);
            } else if (line.contains("DEBUG")) {
                logLevel.set("DEBUG");
                context.write(logLevel, one);
            }
        }
    }

    // Reducer: sums counts for each log level
    public static class LogReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: LogProcessor <input_file> <output_folder>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Processor");

        job.setJarByClass(LogProcessor.class);

        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
================= SIMPLE INSTRUCTIONS =================

1. Save file as:
   LogProcessor.java

2. Create sample log file:
   echo "2024-01-01 INFO Start" > logs.txt
   echo "2024-01-01 ERROR Failure" >> logs.txt
   echo "2024-01-01 WARN Disk low" >> logs.txt
   echo "2024-01-01 INFO Running" >> logs.txt

3. Compile:
   javac -classpath $(hadoop classpath) -d . LogProcessor.java

4. Create jar:
   jar -cvf logprocessor.jar *

5. Run:
   hadoop jar logprocessor.jar LogProcessor logs.txt output

6. View output:
   cat output/part-r-00000

7. If output folder exists:
   rm -r output

=====================================================
*/