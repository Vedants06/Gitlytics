package gitlytics.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sums the counts for each word. Also used as the Combiner: summing is associative
 * and commutative, so partial sums on the map side give the same final result while
 * shuffling far fewer records.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text word, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : counts) {
            sum += count.get();
        }
        total.set(sum);
        context.write(word, total);
    }
}
