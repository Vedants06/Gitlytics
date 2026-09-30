package gitlytics.wordcount;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 2: top-N words from the word-count output ("word \t count" lines).
 * Each mapper keeps only its local top N; one reducer merges them into the global top N.
 * Ties on count are broken alphabetically so the result is deterministic.
 */
public final class TopWords {

    public static final String TOP_N = "gitlytics.top.n";

    private TopWords() {
    }

    /** Orders by count descending, then word ascending. */
    private static String sortKey(long count, String word) {
        return String.format("%019d\t%s", Long.MAX_VALUE - count, word);
    }

    private static void keepTop(TreeMap<String, String> top, long count, String word, int n) {
        top.put(sortKey(count, word), word + "\t" + count);
        if (top.size() > n) {
            top.remove(top.lastKey());
        }
    }

    public static class TopMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final TreeMap<String, String> top = new TreeMap<>();
        private int n;

        @Override
        protected void setup(Context context) {
            n = context.getConfiguration().getInt(TOP_N, 50);
        }

        @Override
        protected void map(LongWritable offset, Text value, Context context) {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                keepTop(top, Long.parseLong(parts[1]), parts[0], n);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String line : top.values()) {
                context.write(NullWritable.get(), new Text(line));
            }
        }
    }

    public static class TopReducer extends Reducer<NullWritable, Text, Text, LongWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int n = context.getConfiguration().getInt(TOP_N, 50);
            TreeMap<String, String> top = new TreeMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                keepTop(top, Long.parseLong(parts[1]), parts[0], n);
            }
            for (Map.Entry<String, String> entry : top.entrySet()) {
                String[] parts = entry.getValue().split("\t");
                context.write(new Text(parts[0]), new LongWritable(Long.parseLong(parts[1])));
            }
        }
    }
}
