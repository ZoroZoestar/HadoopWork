import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerPatternAnalysis {

  public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

      if (columns.length < 8 || columns[0].equalsIgnoreCase("Invoice"))
        return;

      try {

        String customerId = columns[6].trim().replace("\"", "");
        String productName = columns[2].trim().replace("\"", "");

        if (!customerId.isEmpty() && !customerId.contains(".")) {
          context.write(new Text(customerId), new Text(productName));
        }
      } catch (Exception e) {
      }
    }
  }

  public static class CustomerReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Map<String, Integer> productCounts = new HashMap<>();

      for (Text val : values) {
        String product = val.toString();
        productCounts.put(product, productCounts.getOrDefault(product, 0) + 1);
      }

      StringBuilder pattern = new StringBuilder();
      for (Map.Entry<String, Integer> entry : productCounts.entrySet()) {
        pattern.append(entry.getKey()).append("(").append(entry.getValue()).append("), ");
      }

      context.write(key, new Text(pattern.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Customer Purchase Pattern");
    job.setJarByClass(CustomerPatternAnalysis.class);
    job.setMapperClass(CustomerMapper.class);
    job.setReducerClass(CustomerReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}