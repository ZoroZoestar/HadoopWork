import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RevenueAnalysis {

  public static class RevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] columns = line.split(",");

      if (columns[0].equalsIgnoreCase("Invoice"))
        return;

      try {
        String dateOnly = columns[4].split(" ")[0];

        double quantity = Double.parseDouble(columns[3]);
        double price = Double.parseDouble(columns[5]);
        double revenue = quantity * price;

        context.write(new Text(dateOnly), new DoubleWritable(revenue));
      } catch (Exception e) {
      }
    }
  }

  public static class RevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double total = 0;
      for (DoubleWritable val : values) {
        total += val.get();
      }

      double roundedTotal = Math.round(total * 100.0) / 100.0;

      context.write(key, new DoubleWritable(roundedTotal));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Total Revenue per Date");
    job.setJarByClass(RevenueAnalysis.class);
    job.setMapperClass(RevenueMapper.class);
    job.setReducerClass(RevenueReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}