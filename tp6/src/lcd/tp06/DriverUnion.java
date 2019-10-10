package lcd.tp06;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class UnionMapper
extends Mapper<Object, Text, IntWritable, NullWritable> {

	@Override
	public void map(Object o, Text in, Context ctx) 
			throws IOException, InterruptedException
	{
	
		String line = in.toString();
		String w[] = line.split(" ");
		for(String s: w) {
			ctx.write(new IntWritable(Integer.parseInt(s)), NullWritable.get());
		}
		
	}
	
}

class UnionReducer
extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

	@Override
	public void reduce(IntWritable ints, Iterable<NullWritable> nulls,
						Context ctx) 
	throws IOException, InterruptedException {
		ctx.write(ints, NullWritable.get());
	}
	
}
	
public class DriverUnion {

	
	
	public static void main(String[] args)
	
	{
		if (args.length < 2) System.exit(2);
		
		try {
			
			//Création d'une nouvelle configuration
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "lcd.tp06");
		    
					
			
			//Le nom de la classe 'main'
			job.setJarByClass(DriverUnion.class);

			//Le format d'entrée du Map
			//TextInputFormat est un alias pour
			//InputFormat<LongWritable, Text>;
			
			job.setInputFormatClass(TextInputFormat.class);
			
			//Initialisation du Map
			job.setMapperClass(UnionMapper.class);
			
			//Initialisation du Reduce
			job.setReducerClass(UnionReducer.class);

			//Sortie du Map
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			//Sortie (du Reduce)
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
						
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			for(int i = 1; i < args.length; i++)
				FileInputFormat.addInputPath(job, new Path(args[i]));

			
						
			
			System.exit(job.waitForCompletion(true) ?  0 : 1);
			
		} catch (Exception e) {
			System.err.println("Erreur : " + e);
		}
		
	}
	
}
