import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderStatsJob {

    // ======== MAPPER ========
    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, String> userGender = new HashMap<>();
        private Map<String, String> movieNames = new HashMap<>();

        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] files = context.getCacheFiles();

            for (URI file : files) {
                File f = new File(file.getPath());
                BufferedReader br = new BufferedReader(new FileReader(f));

                String line;
                while ((line = br.readLine()) != null) {

                    String[] cols = line.split(",");

                    if (cols.length < 2) continue;

                    // users.txt
                    if (f.getName().toLowerCase().contains("user")) {
                        userGender.put(cols[0].trim(), cols[1].trim().toUpperCase());
                    }

                    // movies.txt
                    else if (f.getName().toLowerCase().contains("movie")) {
                        movieNames.put(cols[0].trim(), cols[1].trim());
                    }
                }
                br.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] cols = value.toString().split(",");

            if (cols.length >= 3) {
                try {
                    String userId = cols[0].trim();
                    String movieId = cols[1].trim();
                    double rating = Double.parseDouble(cols[2].trim());

                    String gender = userGender.get(userId);
                    String title = movieNames.get(movieId);

                    if (gender != null && title != null) {
                        outKey.set(title);
                        outVal.set(gender + "|" + rating);
                        context.write(outKey, outVal);
                    }

                } catch (Exception e) {
                    // skip dòng lỗi
                }
            }
        }
    }

    // ======== REDUCER ========
    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");

                if (parts.length == 2) {
                    String gender = parts[0];
                    double rating;

                    try {
                        rating = Double.parseDouble(parts[1]);
                    } catch (Exception e) {
                        continue;
                    }

                    if ("M".equals(gender)) {
                        maleSum += rating;
                        maleCount++;
                    } else if ("F".equals(gender)) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }

            double maleAvg = maleCount == 0 ? 0 : maleSum / maleCount;
            double femaleAvg = femaleCount == 0 ? 0 : femaleSum / femaleCount;

            result.set("M_avg=" + String.format("%.2f", maleAvg)
                    + " | F_avg=" + String.format("%.2f", femaleAvg));

            context.write(key, result);
        }
    }

    // ======== DRIVER ========
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("Usage: GenderStatsJob <input_dir> <output_dir> <users.txt> <movies.txt>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Stats");

        job.setJarByClass(GenderStatsJob.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(StatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // cache files
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}