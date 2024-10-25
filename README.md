// MissingCardReducer.java 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MissingCardReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text card, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable count : counts) {
            total += count.get();
        }

        if (total == 0) {
            context.write(card, new Text("Missing")); 
        } else {
            context.write(card, new Text("Present")); // Output present cards as well
        }
    }
}
