package lcd.tp06;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class JoinMapper
extends Mapper<Object, Text, IntWritable, BooleanWritable> {

	@Override
	public void map(Object o, Text input, Context ctx) 
			throws IOException, InterruptedException
	{
	
		/*String line = input.toString();
		String ints[] = line.split(" ");
		
		ctx.write(new IntWritable(Integer.parseInt(ints[0])),new  BooleanWritable(false));
		ctx.write(new IntWritable(Integer.parseInt(ints[0])),new  BooleanWritable(true));*/
		FileSplit fs = (FileSplit) ctx.getInputSplit(); //On recupère le fichier que l'on traite dans le context
		String fileName = fs.getPath().getName(); //On recupère le nom du fichier
		
		
	}
	
}

class JoinReducer
extends Reducer<IntWritable, BooleanWritable, IntWritable, NullWritable> {

	@Override
	public void reduce(IntWritable ints, Iterable<BooleanWritable> origin,
						Context ctx) 
	throws IOException, InterruptedException {
		boolean hasLeft = false;
		boolean hasRight = false;
		for(BooleanWritable b : origin) {
			hasLeft |= !b.get();
			hasRight |= b.get();
			
			if(hasLeft && hasRight) {
				ctx.write(ints, NullWritable.get());
				return;
			}
		}
		
	}
	
}
	
public class DriverJoin {

	
	
	public static void main(String[] args)
	
	{
		if (args.length < 2) System.exit(2);
		
		try {
			
			//Création d'une nouvelle configuration
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "lcd.tp06");
		    
					
			
			//Le nom de la classe 'main'
			job.setJarByClass(DriverJoin.class);

			//Le format d'entrée du Map
			//TextInputFormat est un alias pour
			//InputFormat<LongWritable, Text>;
			
			job.setInputFormatClass(TextInputFormat.class);
			
			//Initialisation du Map
			job.setMapperClass(JoinMapper.class);
			
			//Initialisation du Reduce
			job.setReducerClass(JoinReducer.class);

			//Sortie du Map
			job.setMapOutputKeyClass(BooleanWritable.class);
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
