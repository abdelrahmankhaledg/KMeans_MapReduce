
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;

public class Utilities {
    public static ArrayList<String> getInitialCentroids(String dataFilePath, int k){
        ArrayList<String> allData=new ArrayList<>();
        try {
            File dataFile = new File(dataFilePath);
            Scanner reader = new Scanner(dataFile);
            while (reader.hasNextLine()) {
                String dataPoint = reader.nextLine();
                allData.add(dataPoint);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        Random random=new Random();
        ArrayList<String> initialCentroids=new ArrayList<>();
        HashSet<Integer> alreadyPicked=new HashSet<>();
        int i=0;
        while (i<k) {
            int idx=random.nextInt(allData.size());
            if(alreadyPicked.contains(idx)){
                continue;
            }
            initialCentroids.add(allData.get(idx));
            alreadyPicked.add(idx);
            i++;
        }

        return initialCentroids;
    }


    public static Double getDistance(DataPoint d1,DataPoint d2){
        Double distance=Double.valueOf(0);
        ArrayList<Double> firstCoordinates=d1.getCoordinates();
        ArrayList<Double> secondCoordinates=d2.getCoordinates();
        for(int i=0;i<firstCoordinates.size();i++){
            distance+= (firstCoordinates.get(i)- secondCoordinates.get(i))*(firstCoordinates.get(i)- secondCoordinates.get(i));
        }
        return Math.sqrt(distance);
    }

    public static String convertDataPointToString(DataPoint dp){
        StringBuilder dataPointBuilder=new StringBuilder();
        ArrayList<Double> coordinates=dp.getCoordinates();
        for(int i=0;i<coordinates.size();i++){
            dataPointBuilder.append(coordinates.get(i));
            if(i!= coordinates.size()-1)
                dataPointBuilder.append(",");
        }
        return dataPointBuilder.toString();
    }

    public static ArrayList<String> getCentroids(String filepath){
        ArrayList<String> centroids=new ArrayList<>();
        try {
            File dataFile = new File(filepath);
            Scanner reader = new Scanner(dataFile);
            while (reader.hasNextLine()) {
                //First line is the old centroid
                StringBuilder stringBuilder=new StringBuilder();
                stringBuilder.append(reader.nextLine().split("\t")[1].split("#")[0]);
                stringBuilder.append(",centroid");
                centroids.add(stringBuilder.toString());
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return centroids;
    }
    public static boolean checkEqual(ArrayList<String> oldCentroids,ArrayList<String>newCentroids,Double thresh){
        for(int i=0;i< oldCentroids.size();i++){
            DataPoint oldCentroid=new DataPoint(oldCentroids.get(i),1);
            DataPoint newCentroid=new DataPoint(newCentroids.get(i),1);
            Double distance=Utilities.getDistance(oldCentroid,newCentroid);
            if(distance<-1*thresh || distance>thresh)
                return false;
        }
        return true;
    }
}
