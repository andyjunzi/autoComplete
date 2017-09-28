import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;

        @Override
        public void setup(Context context) {
            // how to get the threshold parameter from the configuration?
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // read value from last job
            // cut in between n-gram
            // write out to HDFS
            if (value == null || value.toString().trim().length() == 0) {
                return;
            }
            // in context.write(key, value), the default delimiter between key and value is "\t"
            // e.g. this is cool\t20
            String line = value.toString().trim();

            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length != 2) {
                // two better ways to handle exception:
                // 1.throws Exception()
                // 2.logger.error();
                return;
            }

            // how to filter the n-gram lower than threshold
            int count = Integer.valueOf(wordsPlusCount[1]);
            if (count < threshold) {
                return;
            }
            String[] words = wordsPlusCount[0].split("\\s+");

            // this is --> cool=20
            // what is the outputkey?    outputKey = "words[0, n - 2]"
            // what is the outputvalue?  outputValue = "words[n - 1]=count"
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];

            // write key-value to reducer?
            if (outputKey != null && outputKey.length() >= 1) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        // n means numberOfFollowingWords
        int n;

        // get the n parameter from the configuration
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // can you use priorityQueue to rank topN n-gram, then write out to hdfs?
            // key = this is
            // value = <girl=50, boy=60, cat=90...>
            // TreeMap<count, list(following word)>
            TreeMap<Integer, List<String>> treeMap = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text value : values) {
                String curValue = value.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());

                if (treeMap.containsKey(count)) {
                    treeMap.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    treeMap.put(count, list);
                }
            }

            //<50, <girl, bird>> <60, <boy...>>
            Iterator<Integer> iter = treeMap.keySet().iterator();
            for (int i = 0; iter.hasNext() && i < n; ) {
                int keyCount = iter.next();
                List<String> words = treeMap.get(keyCount);
                for (String curWord : words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
                    i++;
                }
            }
        }
    }
}
