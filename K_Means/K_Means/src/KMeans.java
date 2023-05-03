import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

import javax.xml.bind.SchemaOutputResolver;

public class KMeans {


    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            ArrayList<String> centroidsAsStrings= new ArrayList<>(Arrays.asList(conf.get("c0"), conf.get("c1"), conf.get("c2")));
            ArrayList<DataPoint> centroids=new ArrayList<>();
            for (int i=0;i<centroidsAsStrings.size();i++){
                DataPoint dp=new DataPoint(centroidsAsStrings.get(i),1);
                centroids.add(dp);
            }

            //Compute the distance between the data point and the centroids
            double minDistance=Double.MAX_VALUE;
            int minIndex=0;

            DataPoint dp = new DataPoint(value.toString(),1);

            for(int i=0;i<centroids.size();i++){
                Double distance=Utilities.getDistance(dp,centroids.get(i));
                if(distance<minDistance){
                    minDistance=distance;
                    minIndex=i;
                }
            }
            Text outputKey=new Text(Utilities.convertDataPointToString(centroids.get(minIndex)));

            Text outputValue=new Text(Utilities.convertDataPointToString(dp));

            //The output is the centroid that is closest to the data point as key and the data point as value
            context.write(outputKey,outputValue);
        }
    }

    public static class KMeansCombiner
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Sum of all the data points corresponding to the key
            ArrayList<Double> sum = new ArrayList<>(Collections.nCopies(key.toString().split(",").length,0.0));
            StringBuilder stringBuilder=new StringBuilder();
            int count=0;
            for (Text val : values) {
               DataPoint db=new DataPoint(val.toString(),0);
               count++;
               stringBuilder.append(val);
               stringBuilder.append("#");
               ArrayList<Double> coordinates=db.getCoordinates();
               for(int i=0;i<coordinates.size();i++){
                   sum.set(i,sum.get(i)+coordinates.get(i));
               }
            }

            stringBuilder.append(count);

            DataPoint sumDP=new DataPoint();
            sumDP.setCoordinates(sum);

            stringBuilder.insert(0,"#");
            stringBuilder.insert(0,Utilities.convertDataPointToString(sumDP));

            //The output is a centroid as key and the sum of the data points , the data points themselves and
            //their count as the value
            context.write(key,new Text(stringBuilder.toString()));
        }

    }
    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();
            ArrayList<Double> mean = new ArrayList<>(Collections.nCopies(key.toString().split(",").length,0.0));
            int count=0;
            for (Text val : values) {

                String[] miniCluster=val.toString().split("#");

                //The last element of the minicluster is the number of points in that mini cluster
                count+=Integer.valueOf(miniCluster[miniCluster.length-1]);
                for(int i=1;i<miniCluster.length-1;i++){
                    stringBuilder.append(miniCluster[i]);
                    if(i!=miniCluster.length-2) {
                        stringBuilder.append("#");
                    }
                }
                //The fist element of the mini cluster is the sum of the coordinates of the points in the mini cluster
                DataPoint db=new DataPoint(miniCluster[0],0);
                ArrayList<Double> coordinates=db.getCoordinates();
                for(int i=0;i<coordinates.size();i++){
                    mean.set(i,mean.get(i)+coordinates.get(i));
                }
            }
            for(int i=0;i<mean.size();i++){
                mean.set(i,mean.get(i)/count);
            }


            DataPoint meanDP=new DataPoint();
            meanDP.setCoordinates(mean);

            stringBuilder.insert(0,"#");
            stringBuilder.insert(0,Utilities.convertDataPointToString(meanDP));
            //The output is the old centroid of the cluster as key and the new centroid and the data points in that cluster
            context.write(key,new Text(stringBuilder.toString()));
        }

    }
    public static void main(String[] args) throws Exception {
        Double equalityThresh=1e-6;
        int no_iterations=0;
        ArrayList<String> centroids= Utilities.getInitialCentroids("/home/abdelrahman/Downloads/iris.data",3);
        //centroids= new ArrayList<>(Arrays.asList("4.9,3.1,1.5,0.1,c", "6.0,3.4,4.5,1.6,c", "6.7,3.0,5.0,1.7,c"));
        Path inputPath=new Path(args[1]);
        Path outputPath=new Path(args[2]+"/Iteration_"+no_iterations);
        long start = System.currentTimeMillis();
        int max_iterations=300;
// ...

        for(int i=0;i<max_iterations;i++) {

            System.out.println("Iteration "+no_iterations+" Started");
            no_iterations++;
            Configuration conf = new Configuration();
            conf.set("c0", centroids.get(0));
            conf.set("c1", centroids.get(1));
            conf.set("c2", centroids.get(2));
            Job job = Job.getInstance(conf, "K_Means");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);

            ArrayList<String> oldCentroids = new ArrayList<>(centroids);
            centroids = Utilities.getCentroids(outputPath.toString().substring(outputPath.toString().indexOf(":")+1)+"/part-r-00000");
            if(Utilities.checkEqual(oldCentroids,centroids,equalityThresh)){
                break;
            }
            outputPath=new Path(args[2]+"/Iteration_"+no_iterations);
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("The average running time of the parallel KMeans is "+timeElapsed/1000+" seconds");
        System.exit(0);

        }
}