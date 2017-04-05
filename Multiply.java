package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class M implements Writable {
    public int tag;
	public int row1;
    public double val1;
	
	M () {}

    M (int t,int r, double v ) {
        tag=t; row1 = r;val1 = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(tag);
		out.writeInt(row1);
        out.writeDouble(val1);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag= in.readInt();
		row1 = in.readInt();
        val1 = in.readDouble();
    }
}


class Result implements Writable {
	public double matvalue;

    Result (Double midval) {
        matvalue = midval;
    }
	Result(){}

    public void write ( DataOutput out ) throws IOException {
		out.writeDouble(matvalue);
    }

    public void readFields ( DataInput in ) throws IOException {
        matvalue = in.readDouble();
    }

    public String toString () { return Double.toString(matvalue); }
}

class Pair implements WritableComparable<Pair>{
	public int i;
	public int j;

    Pair (int m,int n) {
        i = m;
		j = n;
    }
	Pair(){}

    public void write ( DataOutput out ) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
		j = in.readInt();
    }

    public String toString () { return i+","+j; }
	
	@Override
	public int compareTo(Pair a)
	{
		if (i==a.i)
		{
			if (j == a.j)
				return 0;
			else if(j < a.j)
				return -1;
			else
				return 1;
		}
		else if(i<a.i)
			return -1;
		else 
			return 1;
	}
	
}

public class Multiply {
	public static class MMapper extends Mapper<Object,Text,IntWritable,M > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int rowm = s.nextInt();
			int colm = s.nextInt();
			double valm = s.nextDouble();
            M mr = new M(0,rowm,valm);
			String keys = Integer.toString(colm);
            context.write(new IntWritable(colm),mr);
            s.close();
        }
    }

    public static class NMapper extends Mapper<Object,Text,IntWritable,M > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int rown = s.nextInt();
			int coln = s.nextInt();
			double valn = s.nextDouble();
            M nr = new M(1,coln,valn);
			String keys = Integer.toString(rown);
            context.write(new IntWritable(rown),nr);
            s.close();
        }
    }

    public static class ResultReducer1 extends Reducer<IntWritable,M,Pair,Result> {
        static Vector<M> mmat = new Vector<M>();
        static Vector<M> nmat = new Vector<M>();
        @Override
        public void reduce ( IntWritable key, Iterable<M> values, Context context )
                           throws IOException, InterruptedException {
            mmat.clear();
            nmat.clear();
            for (M v: values)
                if (v.tag == 0)
				{
					M m = new M(0,v.row1,v.val1);
					mmat.add(m);
				}
                    
                else 
				{
					M n = new M(1,v.row1,v.val1);
					nmat.add(n);
				}
				
			 for (M m:mmat)
			 {
				 for( M n : nmat)
				 {		Pair p=new Pair(m.row1,n.row1);			
						context.write(p,new Result(m.val1*n.val1));						
				}
			}				
        }
    }
	
	public static class InterMapper extends Mapper<Pair,Result,Pair,Result > {
        @Override
        public void map ( Pair key, Result value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class ResultReducer2 extends Reducer<Pair,Result,Text,DoubleWritable> {
       
        public void reduce ( Pair key, Iterable<Result> values, Context context )
                           throws IOException, InterruptedException {
            
			double finalvalue= 0.0;
            for (Result v: values){
					finalvalue= finalvalue + v.matvalue;
			}
			String keyf= Integer.toString(key.i)+","+Integer.toString(key.j);
			context.write(new Text(keyf),new DoubleWritable(finalvalue));
		}
    }
	
	public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Matrix Multiplication");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Result.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(M.class);
        job.setReducerClass(ResultReducer1.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,NMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.setNumReduceTasks(2);
		job.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance();
		job2.setJobName("Aggregating Values");
		job2.setJarByClass(Multiply.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Result.class);
		job2.setReducerClass(ResultReducer2.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
		MultipleInputs.addInputPath(job2,new Path(args[2]),SequenceFileInputFormat.class,InterMapper.class);
		FileOutputFormat.setOutputPath(job2,new Path(args[3]));
		job2.waitForCompletion(true);
    }
}
