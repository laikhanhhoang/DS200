import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeStatsJob {

    // ===== MAPPER =====
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, String> userAgeGroup = new HashMap<>();
        private Map<String, String> movieNames = new HashMap<>();

        private Text outKey = new Text();
        private Text outVal = new Text();

        // SỬA LẠI: Phân loại đúng theo nhóm tuổi yêu cầu
        private String classifyAge(int age) {
            if (age <= 18) return "0-18";
            else if (age <= 35) return "18-35";
            else if (age <= 50) return "35-50";
            else return "50+";
        }

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

                    if (f.getName().toLowerCase().contains("user")) {
                        try {
                            int age = Integer.parseInt(cols[2].trim());
                            userAgeGroup.put(cols[0].trim(), classifyAge(age));
                        } catch (Exception e) {}
                    }

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

                    String ageGroup = userAgeGroup.get(userId);
                    String title = movieNames.get(movieId);

                    if (ageGroup != null && title != null) {
                        outKey.set(title);
                        outVal.set(ageGroup + "|" + rating);
                        context.write(outKey, outVal);
                    }

                } catch (Exception e) {}
            }
        }
    }

    // ===== REDUCER =====
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> sum = new HashMap<>();
            Map<String, Integer> count = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");

                if (parts.length != 2) continue;

                String group = parts[0];
                double rating;

                try {
                    rating = Double.parseDouble(parts[1]);
                } catch (Exception e) {
                    continue;
                }

                sum.put(group, sum.getOrDefault(group, 0.0) + rating);
                count.put(group, count.getOrDefault(group, 0) + 1);
            }

            // SỬA LẠI: build output format giống y hệt màn hình
            StringBuilder sb = new StringBuilder();
            List<String> ageGroups = Arrays.asList("0-18", "18-35", "35-50", "50+");

            for (int i = 0; i < ageGroups.size(); i++) {
                String group = ageGroups.get(i);
                sb.append(group).append(": ");
                
                // Nếu nhóm tuổi này có đánh giá phim -> Tính trung bình
                if (count.containsKey(group)) {
                    double avg = sum.get(group) / count.get(group);
                    // Dùng Locale.US để đảm bảo output ra dấu chấm (ví dụ: 3.75) thay vì dấu phẩy
                    sb.append(String.format(Locale.US, "%.2f", avg));
                } else {
                    // Nếu không có ai trong nhóm tuổi này đánh giá -> In ra NA
                    sb.append("NA");
                }

                // Thêm một khoảng tab (\t) để cách các nhóm tuổi ra cho dễ nhìn
                if (i < ageGroups.size() - 1) {
                    sb.append("\t");
                }
            }

            result.set(sb.toString());
            context.write(key, result);
        }
    }

    // ===== DRIVER =====
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("Usage: AgeStatsJob <input> <output> <users> <movies>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Stats");

        job.setJarByClass(AgeStatsJob.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}