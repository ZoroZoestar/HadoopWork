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

  public static class RegionProductMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] columns = line.split(",");

      if (columns[0].equalsIgnoreCase("Invoice"))
        return;

      try {
        String productName = columns[2].trim();
        String region = columns[7].trim();
        int quantity = Integer.parseInt(columns[3]);

        String compositeKey = region + " | " + productName;

        context.write(new Text(compositeKey), new IntWritable(quantity));
      } catch (Exception e) {
      }
    }
  }

  public static class RegionProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int totalQuantity = 0;
      for (IntWritable val : values) {
        totalQuantity += val.get();
      }
      context.write(key, new IntWritable(totalQuantity));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Top Product per Region");
    job.setJarByClass(ProductRegionAnalysis.class);
    job.setMapperClass(RegionProductMapper.class);
    job.setReducerClass(RegionProductReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}