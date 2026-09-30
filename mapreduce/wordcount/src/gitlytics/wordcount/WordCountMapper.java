package gitlytics.wordcount;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Emits (word, 1) for every word in a commit message.
 *
 * Input format (gitlytics.input.format):
 *   silver - one commit per line from /gitlytics/silver/commits (tab-separated)
 *   raw    - one GitHub event per line from the bronze .json.gz files
 *
 * Cleaning: lower-case, letters only, length >= 3, stop-words removed,
 * bot commits skipped (gitlytics.skip.bots, default true).
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public enum Counter { RECORDS, COMMITS, BOT_COMMITS_SKIPPED, BAD_RECORDS, WORDS_EMITTED, STOPWORDS_SKIPPED, SHORT_TOKENS_SKIPPED }

    // silver commits columns: repo_owner, repo, actor_login, is_bot, sha, message, created_at
    private static final int COL_IS_BOT = 3;
    private static final int COL_MESSAGE = 5;
    private static final int MIN_WORD_LENGTH = 3;

    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();
    private final Set<String> stopwords = new HashSet<>();
    private final ObjectMapper json = new ObjectMapper();
    private boolean rawInput;
    private boolean skipBots;

    @Override
    protected void setup(Context context) throws IOException {
        rawInput = "raw".equals(context.getConfiguration().get("gitlytics.input.format", "silver"));
        skipBots = context.getConfiguration().getBoolean("gitlytics.skip.bots", true);

        // stopwords.txt is shipped to every task through the distributed cache (-files)
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null) {
            for (URI uri : cacheFiles) {
                String name = new Path(uri.getPath()).getName();
                if (name.equals("stopwords.txt")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(name))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            line = line.trim().toLowerCase();
                            if (!line.isEmpty() && !line.startsWith("#")) {
                                stopwords.add(line);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.RECORDS).increment(1);
        if (rawInput) {
            mapRawEvent(value.toString(), context);
        } else {
            mapSilverCommit(value.toString(), context);
        }
    }

    private void mapSilverCommit(String line, Context context) throws IOException, InterruptedException {
        String[] fields = line.split("\t", -1);
        if (fields.length <= COL_MESSAGE) {
            context.getCounter(Counter.BAD_RECORDS).increment(1);
            return;
        }
        context.getCounter(Counter.COMMITS).increment(1);
        if (skipBots && "true".equals(fields[COL_IS_BOT])) {
            context.getCounter(Counter.BOT_COMMITS_SKIPPED).increment(1);
            return;
        }
        emitWords(fields[COL_MESSAGE], context);
    }

    private void mapRawEvent(String line, Context context) throws IOException, InterruptedException {
        JsonNode event;
        try {
            event = json.readTree(line);
        } catch (IOException e) {
            context.getCounter(Counter.BAD_RECORDS).increment(1);
            return;
        }
        if (!"PushEvent".equals(event.path("type").asText())) {
            return;
        }
        boolean bot = event.path("actor").path("login").asText().endsWith("[bot]");
        for (JsonNode commit : event.path("payload").path("commits")) {
            context.getCounter(Counter.COMMITS).increment(1);
            if (skipBots && bot) {
                context.getCounter(Counter.BOT_COMMITS_SKIPPED).increment(1);
                continue;
            }
            emitWords(commit.path("message").asText(""), context);
        }
    }

    private void emitWords(String message, Context context) throws IOException, InterruptedException {
        for (String token : message.toLowerCase().split("[^a-z]+")) {
            if (token.length() < MIN_WORD_LENGTH) {
                if (!token.isEmpty()) {
                    context.getCounter(Counter.SHORT_TOKENS_SKIPPED).increment(1);
                }
                continue;
            }
            if (stopwords.contains(token)) {
                context.getCounter(Counter.STOPWORDS_SKIPPED).increment(1);
                continue;
            }
            word.set(token);
            context.write(word, ONE);
            context.getCounter(Counter.WORDS_EMITTED).increment(1);
        }
    }
}
