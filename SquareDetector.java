import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

// MapReduce code to count the rectangles in a graph.
public class SquareDetector extends Configured implements Tool {
	// Maps values to (Long,Long) pairs.
	public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		LongWritable mkey = new LongWritable();
        LongWritable mval = new LongWritable();

		public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long src, dst;

            if (tokenizer.hasMoreTokens()) {
            	src = Long.parseLong(tokenizer.nextToken());
            	if (!tokenizer.hasMoreTokens())
            		throw new RuntimeException("Invalid edge line: " + line);
            	dst = Long.parseLong(tokenizer.nextToken());

            	mkey.set(src);
            	mval.set(dst);
            	context.write(mkey, mval);
                context.write(mval, mkey);
            }
        }
	}

	// Creates the triads for each apex.
    public static class PivotingAroundReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        // Produces triads, i.e. pairs where the first item is a node and the second item is a pair of its neighbours.
    	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
            ArrayList<Long> storage = new ArrayList<Long>();
        	Iterator<LongWritable> itr = values.iterator();

        	while (itr.hasNext())
        		storage.add(itr.next().get());

            // Emits low and mixed triads.
        	if (storage.size() > 1) {
                Collections.sort(storage);
                for (int i = 0; i < storage.size() - 1; i++) {
                    for (int j = i + 1; j < storage.size(); j++) {
                        if (key.get() < storage.get(j))
                            context.write(key, new Text(storage.get(i).toString() + "," + storage.get(j).toString()));
                    }
                }
            }
        }
    }

    public static class TriadsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Text mkey = new Text();
        LongWritable mval = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String k, v;
            if (tokenizer.hasMoreTokens()) {
                k = tokenizer.nextToken();
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("Invalid edge line: " + line);
                v = tokenizer.nextToken();
                if (v.contains(",")) {
                    mkey.set(v);
                    mval.set(Long.parseLong(k));
                }
                else {
                    mkey.set(k + "," + v);
                    mval.set(-1);
                }
                context.write(mkey, mval);
            }
        }
    }

    // Emits distinct triads sharing common ends.
    public static class SquareReducer extends Reducer<Text, LongWritable, Text, Text> {
        // Produces pairs where the first item is the vertex key and the second item is a single distinct square.
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
            ArrayList<Long> storage = new ArrayList<Long>();
            Iterator<LongWritable> itr = values.iterator();

            while (itr.hasNext())
                storage.add(itr.next().get());

            if (storage.size() > 1) {
                Collections.sort(storage);
                if (storage.get(0) != -1) {
                    for (int i = 0; i < storage.size() - 1; i++) {
                        for (int j = i + 1; j < storage.size(); j++)
                            context.write(key, new Text(storage.get(i).toString() + "," + storage.get(j).toString()));
                    }
                }
            }
        }
    }

    // Detects wrongful rectangles.
    public static class EndsMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text mKey = new Text();
        Text mVal = new Text();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String k, v;
            if (tokenizer.hasMoreTokens()) {
                k = tokenizer.nextToken();
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("Invalid edge line: " + line);
                v = tokenizer.nextToken();
                if (v.contains(",")) {
                    mKey.set(v);
                    mVal.set(k);
                }
                else {
                    mKey.set(k + "," + v);
                    mVal.set("-1");
                }
                context.write(mKey, mVal);
            }
        }
    }

    // Emits partitions.
    public static class PartitionReducer extends Reducer<Text, Text, Text, Text> {
        // Produces pairs where the first item is one set partition and the second item is the other one.
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            ArrayList<String> storage = new ArrayList<String>();
            Iterator<Text> itr = values.iterator();

            while (itr.hasNext())
                storage.add(itr.next().toString());

            if (!storage.contains("-1")) {
                for (int i = 0; i < storage.size(); i++)
                    context.write(new Text(storage.get(i).toString()), key);
            }
        }
    }

    // Takes two arguments, the edges file and output file.
    // File pattern must be composed of lines of the form:
    //   long <whitespace> long <newline>
    // and each long must be a positive integer.
    public int run(String[] args) throws Exception {
    	Job job1 = new Job(getConf());
        job1.setJobName("triads");

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setJarByClass(SquareDetector.class);
        job1.setMapperClass(ParseLongLongPairsMapper.class);
        job1.setReducerClass(PivotingAroundReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("squarecounter/tmp1"));


        Job job2 = new Job(getConf());
        job2.setJobName("squares");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setJarByClass(SquareDetector.class);
        job2.setMapperClass(TriadsMapper.class);
        job2.setReducerClass(SquareReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileInputFormat.addInputPath(job2, new Path("squarecounter/tmp1"));
        FileOutputFormat.setOutputPath(job2, new Path("squarecounter/tmp2"));

        Job job3 = new Job(getConf());
        job3.setJobName("partitions");

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setJarByClass(SquareDetector.class);
        job3.setMapperClass(EndsMapper.class);
        job3.setReducerClass(PartitionReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileInputFormat.addInputPath(job3, new Path("squarecounter/tmp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));


        int outcome = job1.waitForCompletion(true) ? 0 : 1;
        if (outcome == 0) outcome = job2.waitForCompletion(true) ? 0 : 1;
        if (outcome == 0) outcome = job3.waitForCompletion(true) ? 0 : 1;
        return outcome;
    }

    public static void main(String[] args) throws Exception {
        int outcome = ToolRunner.run(new Configuration(), new SquareDetector(), args);
        System.exit(outcome);
    }
}
