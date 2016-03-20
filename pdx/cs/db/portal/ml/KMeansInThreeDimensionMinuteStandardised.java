package pdx.cs.db.portal.ml;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;

public class KMeansInThreeDimensionMinuteStandardised {
	
	int NO_OF_FIRST_LEVEL_CLUSTERS = 7;
	int ITERATIONS = 100;
	DecimalFormat df = new DecimalFormat("#.00");
	
	List<Clusterable> points = new ArrayList<Clusterable>();
	KMeansPlusPlusClusterer<Clusterable> kMeanClusterer = new KMeansPlusPlusClusterer<Clusterable>(NO_OF_FIRST_LEVEL_CLUSTERS, ITERATIONS);
	
	//2nd lane on Cedar Hills (2R352) to WB US 26, Murray (2R354) to WB US 26, Bethany (2R356) to WB US 26 
	String sqlRadar = "select speed, volume, extract(HOUR from starttime)*60 + extract(MINUTE from starttime) as minute_of_day "
		     + "from freeway.data "
		    + "where starttime between '2015-06-15 12:00:00-00' and '2015-09-15 00:00:00-00' "
		    + "and detectorid in (100874,100871,100868) "
		    + "and extract(HOUR from starttime) between 6 and 20 "
		    + "order by starttime";
	
	//2nd lane on 207th (2DS025) @ WB I-84 MP14.4, 223rd (2DS026) @ WB I-84 MP15.17, 82nd/Halsey (2R080) to WB I-84 
	String sqlLoop = "select speed, volume, extract(HOUR from starttime)*60 + extract(MINUTE from starttime)  as minute_of_day "
		     + "from freeway.data "
		    + "where starttime between '2015-06-15 12:00:00-00' and '2015-09-15 00:00:00-00' "
		    + "and detectorid in (101178,101181,100901) "
		    + "and extract(HOUR from starttime) between 6 and 20 "
		    + "order by starttime";	

	public static void main(String[] args) throws Exception {
		KMeansInThreeDimensionMinuteStandardised kMeansInThreeDimensionMinuteStandardised = new KMeansInThreeDimensionMinuteStandardised();
		kMeansInThreeDimensionMinuteStandardised.runKMeanAlgorithm();
	}	
	
	private void runKMeanAlgorithm() throws Exception {
		readAndStoreObservations(sqlRadar);
		
		System.out.println("Radar data read from database");
		
		List<CentroidCluster<Clusterable>> radarClusters =  kMeanClusterer.cluster(points);
		
		System.out.println("Clusters created for radar detectors.");
		
		String filename = "CSV_FILES//ThreeDimentionalKMeansOutputForRadarDetectorsMinuteStd_X.csv";
		int clusterCount = 0;
		for (CentroidCluster<Clusterable> cluster : radarClusters) {
			double[] centroid = cluster.getCenter().getPoint();
			int numberOfObservations = cluster.getPoints().size();
			
			System.out.print("Radar Centroid: ");
			for (int i = 0; i < centroid.length; i++) {
				System.out.print(new Double(df.format(centroid[i])) + " ");
			}
			System.out.print("Number of observations: " + numberOfObservations);
			System.out.println("");
			
			createBubbleChartData(filename,clusterCount, cluster.getPoints(), cluster.getCenter().getPoint());
			clusterCount++;
		}		
		
		System.out.println("Radar data generated for plotting.");
		
		points = new ArrayList<Clusterable>();
		readAndStoreObservations(sqlLoop);
		
		System.out.println("Wave data read from database");
		
		List<CentroidCluster<Clusterable>> waveClusters =  kMeanClusterer.cluster(points);
		
		System.out.println("Clusters created for wave detectors.");
		
		filename= "CSV_FILES//ThreeDimentionalKMeansOutputForWaveDetectorsMinuteStd_X.csv";
		clusterCount = 0;
		for (CentroidCluster<Clusterable> cluster : waveClusters) {
			double[] centroid = cluster.getCenter().getPoint();
			int numberOfObservations= cluster.getPoints().size();
			
			System.out.print("Wave Centroid: ");
			for (int i = 0; i < centroid.length; i++) {
				System.out.print(new Double(df.format(centroid[i])) + " ");
			}
			System.out.print("Number of observations: " + numberOfObservations);
			System.out.println("");
			
			createBubbleChartData(filename, clusterCount, cluster.getPoints(), cluster.getCenter().getPoint());
			clusterCount++;
		}		
		
		System.out.println("Wave data generated for plotting.");		
	}

	private void readAndStoreObservations(String sql) throws Exception {
		String url = "jdbc:postgresql://portaldb.its.pdx.edu/portals?user=userid&password=password";
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery(sql);
		
		ArrayList<double[]> allPoints = new ArrayList<double[]>();
		
		while(rs.next()) {
			double speed = Math.ceil(rs.getDouble("speed"));
			double volume = rs.getDouble("volume");
			double minuteOfDay = rs.getDouble("minute_of_day");
			
			double[] point = {speed,volume, minuteOfDay};
			allPoints.add(point);
		}
		
		//convert list of double[] to two-dimensional array
		double[][] points_2d = new double[allPoints.size()][3];
		for(int i = 0; i < allPoints.size(); i++) {
			points_2d[i] = allPoints.get(i); 
		}
		
		double[][] standardisedPoints = standardizeAllPoints(points_2d);
		
		for (int i = 0; i < standardisedPoints.length; i++) {
			createClusterablePoints(standardisedPoints[i]);
		}
	}
	
	private double[][] standardizeAllPoints(double[][] points_2d) {
		double[] speedArray = new double[points_2d.length];
		double[] volumeArray = new double[points_2d.length];
		double[] minutesArray = new double[points_2d.length];;
		
		for (int i = 0; i < points_2d.length; i++) {
			speedArray[i] = points_2d[i][0];
		}
		
		for (int i = 0; i < minutesArray.length; i++) {
			volumeArray[i] = points_2d[i][1];
		}
		
		for (int i = 0; i < minutesArray.length; i++) {
			minutesArray[i] = points_2d[i][2];
		}
		
		Mean mean = new Mean();
		StandardDeviation sd = new StandardDeviation();
		
		double meanSpeed = mean.evaluate(speedArray);
		double meanVolume = mean.evaluate(volumeArray);
		double meanMinutes = mean.evaluate(minutesArray);
		double sdSpeed = sd.evaluate(speedArray);
		double sdVolume = sd.evaluate(volumeArray);
		double sdMinutes = sd.evaluate(minutesArray);
		
		double[][] standardisedPoints = new double[points_2d.length][3];
		
		for (int i = 0; i < points_2d.length; i++) {
			standardisedPoints[i][0] = (points_2d[i][0] - meanSpeed)/sdSpeed; 
			standardisedPoints[i][1] = (points_2d[i][1] - meanVolume)/sdVolume;
			standardisedPoints[i][2] = (points_2d[i][2] - meanMinutes)/sdMinutes;
		}
		
		double minSpeed_2d = Double.MAX_VALUE;
		double maxSpeed_2d = Double.MIN_VALUE;
		double minVolume_2d = Double.MAX_VALUE;
		double maxVolume_2d = Double.MIN_VALUE;
		double minTime_2d = Double.MAX_VALUE;
		double maxTime_2d = Double.MIN_VALUE;


		for (int i = 0; i <  points_2d.length; i++) {
			if( points_2d[i][0] < minSpeed_2d) 	minSpeed_2d =  points_2d[i][0];
			if( points_2d[i][0] > maxSpeed_2d) 	maxSpeed_2d =  points_2d[i][0];
			if( points_2d[i][1] < minVolume_2d) 	minVolume_2d =  points_2d[i][1];
			if( points_2d[i][1] > maxVolume_2d) 	maxVolume_2d =  points_2d[i][1];
			if( points_2d[i][2] < minTime_2d) 		minTime_2d =  points_2d[i][2];
			if( points_2d[i][2] > maxTime_2d) 		maxTime_2d =  points_2d[i][2];
		}		
		
		double minSpeed = Double.MAX_VALUE;
		double maxSpeed = Double.MIN_VALUE;
		double minVolume = Double.MAX_VALUE;
		double maxVolume = Double.MIN_VALUE;
		double minTime = Double.MAX_VALUE;
		double maxTime = Double.MIN_VALUE;
		
		for (int i = 0; i < standardisedPoints.length; i++) {
			if(standardisedPoints[i][0] < minSpeed) 	minSpeed = standardisedPoints[i][0];
			if(standardisedPoints[i][0] > maxSpeed) 	maxSpeed = standardisedPoints[i][0];
			if(standardisedPoints[i][1] < minVolume) 	minVolume = standardisedPoints[i][1];
			if(standardisedPoints[i][1] > maxVolume) 	maxVolume = standardisedPoints[i][1];
			if(standardisedPoints[i][2] < minTime) 		minTime = standardisedPoints[i][2];
			if(standardisedPoints[i][2] > maxTime) 		maxTime = standardisedPoints[i][2];
		}
		
		System.out.println(" mean speed:" + meanSpeed);
		System.out.println("min speed original: " + minSpeed_2d + ", min speed standaedized: " + minSpeed);
		System.out.println("max speed original: " + maxSpeed_2d + ", max speed standaedized: " + maxSpeed);
		System.out.println(" mean volume:" + meanVolume);
		System.out.println("min volume original: " + minVolume_2d + ", min speed standaedized: " + minVolume);
		System.out.println("max volume original: " + maxVolume_2d + ", max speed standaedized: " + maxVolume);
		System.out.println(" mean time:" + meanMinutes);
		System.out.println("min time original: " + minTime_2d + ", min time standaedized: " + minTime);
		System.out.println("max time original: " + maxTime_2d + ", max time standaedized: " + maxTime);		
		
		return standardisedPoints;
	}
		
	private void createClusterablePoints(double[] point) {
		DoublePoint threeDimObservation = new DoublePoint(point); 
		points.add(threeDimObservation);
	}
	
	private void createBubbleChartData(String filename, int clusterCount, List<Clusterable> points, double[] centroid) throws Exception {
		
		filename = filename.replace("X", clusterCount + "");
		PrintWriter out = new PrintWriter(filename);
		
		HashMap<Point_3d, Integer> pointWithIntensity = new HashMap<Point_3d, Integer>();
		
		for (Clusterable clusterable : points) {
			double x = clusterable.getPoint()[0];
			double y = clusterable.getPoint()[1];
			double z = clusterable.getPoint()[2];
			
			Point_3d currentPoint = new Point_3d(x, y, z);
			
			Integer existingPointCount = pointWithIntensity.get(currentPoint);
			
			if(existingPointCount == null) {
				pointWithIntensity.put(currentPoint, 1);
			}else {
				pointWithIntensity.put(currentPoint, existingPointCount + 1);
			}
		}
		
		out.println("Speed: " + new Double(df.format(centroid[0])) + " volume: " + new Double(df.format(centroid[1])) + " minute of day: " + new Double(df.format(centroid[2])));
		
		Iterator<Point_3d> points_3dIter = pointWithIntensity.keySet().iterator();
		while(points_3dIter.hasNext()) {
			Point_3d point_3d = points_3dIter.next();
			out.println(point_3d.getX() + "," + point_3d.getY() + "," + point_3d.getZ() + "," + pointWithIntensity.get(point_3d));
		}
		
		out.close();
		
	}
}
