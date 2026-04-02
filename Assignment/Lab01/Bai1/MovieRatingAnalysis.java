import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MovieRatingAnalysis {

    // ================= MAPPER =================
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private Text movieId = new Text();
        private FloatWritable rating = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("::"); // ✅ FIX

            if (parts.length >= 3) {
                try {
                    movieId.set(parts[1]);
                    rating.set(Float.parseFloat(parts[2]));
                    context.write(movieId, rating);
                } catch (Exception e) {
                    // skip dòng lỗi
                }
            }
        }
    }

    // ================= REDUCER =================
    public static class MovieReducer extends Reducer<Text, FloatWritable, Text, Text> {

        private Map<String, String> movieMap = new HashMap<>();

        private String bestMovie = "";
        private float bestAvg = -1;

        @Override
        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles == null) {
                System.out.println("No cache file found!");
                return;
            }

            for (URI uri : cacheFiles) {

                try (BufferedReader br =
                             new BufferedReader(new FileReader(new File(uri.getPath())))) {

                    String line;
                    while ((line = br.readLine()) != null) {

                        String[] parts = line.split("::"); // ✅ FIX

                        if (parts.length >= 2) {
                            movieMap.put(parts[0], parts[1]);
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            float sum = 0;

            for (FloatWritable v : values) {
                sum += v.get();
                count++;
            }

            if (count == 0) return;

            float avg = sum / count;

            String title = movieMap.getOrDefault(key.toString(), "Unknown");

            context.write(
                    new Text(title),
                    new Text(String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count))
            );

            // tìm phim tốt nhất
            if (count >= 5 && avg > bestAvg) {
                bestAvg = avg;
                bestMovie = title;
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (!bestMovie.isEmpty()) {
                context.write(
                        new Text(""),
                        new Text(bestMovie + " is the highest rated movie with an average rating of "
                                + String.format("%.2f", bestAvg)
                                + " among movies with at least 5 ratings.")
                );
            } else {
                context.write(
                        new Text("No movie"),
                        new Text("has at least 5 ratings.")
                );
            }
        }
    }

    // ================= DRIVER =================
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("Usage: MovieRatingAnalysis <rating1> <rating2> <output> <movies>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(MovieRatingAnalysis.class);

        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 2 input files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // cache movies
        job.addCacheFile(new Path(args[3]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}