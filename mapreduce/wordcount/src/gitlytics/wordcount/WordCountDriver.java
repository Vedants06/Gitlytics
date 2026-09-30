package gitlytics.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Lab experiment 2: word count on GitHub commit messages, chained into a top-N job.
 *
 *   hadoop jar wordcount.jar gitlytics.wordcount.WordCountDriver \
 *       -files config/stopwords.txt [-D gitlytics.input.format=raw] <input> <output>
 *
 * Writes <output>/counts (every word) and <output>/top (top 50 by count).
 */
public class WordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: WordCountDriver [generic options] <input> <output>");
            return 2;
        }
        Configuration conf = getConf();
        Path input = new Path(args[0]);
        Path counts = new Path(args[1], "counts");
        Path top = new Path(args[1], "top");
        FileSystem.get(conf).delete(new Path(args[1]), true);

        // Job 1: word count with a combiner
        Job count = Job.getInstance(conf, "gitlytics commit word count (" + conf.get("gitlytics.input.format", "silver") + ")");
        count.setJarByClass(WordCountDriver.class);
        count.setMapperClass(WordCountMapper.class);
        count.setCombinerClass(WordCountReducer.class);
        count.setReducerClass(WordCountReducer.class);
        count.setNumReduceTasks(conf.getInt("gitlytics.reducers", 2));
        count.setOutputKeyClass(Text.class);
        count.setOutputValueClass(IntWritable.class);
        // Inputs are partitioned folders (dt=/hr=), so read sub-directories too
        FileInputFormat.setInputDirRecursive(count, true);
        FileInputFormat.addInputPath(count, input);
        FileOutputFormat.setOutputPath(count, counts);
        if (!count.waitForCompletion(true)) {
            return 1;
        }

        // Job 2: global top N from the counts
        Job rank = Job.getInstance(conf, "gitlytics commit top words");
        rank.setJarByClass(WordCountDriver.class);
        rank.setMapperClass(TopWords.TopMapper.class);
        rank.setReducerClass(TopWords.TopReducer.class);
        rank.setNumReduceTasks(1);
        rank.setMapOutputKeyClass(NullWritable.class);
        rank.setMapOutputValueClass(Text.class);
        rank.setOutputKeyClass(Text.class);
        rank.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(rank, counts);
        FileOutputFormat.setOutputPath(rank, top);
        return rank.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WordCountDriver(), args));
    }
}
