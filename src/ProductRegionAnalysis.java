import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductRegionAnalysis {

  public static class RegionProductMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

      if (columns.length < 8 || columns[0].equalsIgnoreCase("Invoice"))
        return;

      try {
        String region = columns[7].trim();
        String productName = columns[2].trim();
        String quantity = columns[3].trim();

        context.write(new Text(region), new Text(productName + "###" + quantity));
      } catch (Exception e) {
      }
    }
  }

  public static class RegionProductReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      java.util.Map<String, Integer> productCounts = new java.util.HashMap<>();

      for (Text val : values) {
        String[] parts = val.toString().split("###");
        if (parts.length < 2)
          continue;

        String prodName = parts[0];
        try {
          int qty = Integer.parseInt(parts[1]);
          productCounts.put(prodName, productCounts.getOrDefault(prodName, 0) + qty);
        } catch (NumberFormatException e) {
        }
      }

      String topProduct = "";
      int maxQty = -1;
      for (java.util.Map.Entry<String, Integer> entry : productCounts.entrySet()) {
        if (entry.getValue() > maxQty) {
          maxQty = entry.getValue();
          topProduct = entry.getKey();
        }
      }

      context.write(key, new Text("-> " + topProduct + " (Total: " + maxQty + ")"));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Top Product per Region");
    job.setJarByClass(ProductRegionAnalysis.class);

    job.setMapperClass(RegionProductMapper.class);
    job.setReducerClass(RegionProductReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
