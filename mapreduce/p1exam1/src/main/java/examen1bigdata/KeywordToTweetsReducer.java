package examen1bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String ids = "";


        for (Text id : values) {
            ids += id.toString()+" ";
        }
        context.write(key, new Text(ids));
    }
}
