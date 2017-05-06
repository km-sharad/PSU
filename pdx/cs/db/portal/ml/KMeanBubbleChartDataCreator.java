package pdx.cs.db.portal.ml;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.evaluation.SumOfClusterVariances;

public class KMeanBubbleChartDataCreator {
	
	int NO_OF_CLUSTERS = 8;
	int ITERATIONS = 100;
	
	List<Clusterable> points = new ArrayList<Clusterable>();
	KMeansPlusPlusClusterer<Clusterable> kMeanClusterer = new KMeansPlusPlusClusterer<Clusterable>(NO_OF_CLUSTERS, ITERATIONS);
	SumOfClusterVariances<Clusterable> sumOfClusterVariances = new SumOfClusterVariances<Clusterable>(new org.apache.commons.math3.ml.distance.EuclideanDistance());
	
	
	public static void main(String[] args)  throws Exception {
		KMeanBubbleChartDataCreator kMeanImplementor = new KMeanBubbleChartDataCreator();
		kMeanImplementor.runKMeanAlgorithm();
	}
	
	private void runKMeanAlgorithm() throws Exception {
		readAndStoreObservations();
		
		List<CentroidCluster<Clusterable>> clusters =  kMeanClusterer.cluster(points);
		
		printStats(clusters);

		for (CentroidCluster<Clusterable> centroidCluster : clusters) {
			outputBubbleChartData(centroidCluster.getPoints(), centroidCluster.getCenter().getPoint());
		}
	}

	private void readAndStoreObservations() throws Exception {
		String url = "jdbc:postgresql://portaldb.its.pdx.edu/portals?user=userid&password=password";
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		
		/*
		 * Query to analyze 2nd lane data for detectors on 
		 * 100874 = Cedar Hill, 100871 = Murray, 100868 = Bethany
		 */
		
		String sql = "select speed, volume "
				     + "from freeway.data "
				    + "where starttime between '2015-06-15 12:00:00-00' and '2015-09-15 00:00:00-00' "
				    + "and detectorid in (100874,100871,100868) "
				    + "order by starttime";
		
		ResultSet rs = stmt.executeQuery(sql);
		
		while(rs.next()) {
			double speed = Math.ceil(rs.getDouble("speed"));
			double volume = rs.getInt("volume");
			
			createClusterablePoints(speed, volume);
		}
	}	
	
	private void createClusterablePoints(double speed, double volume) {
		double[] point = new double[] {speed,volume};
		DoublePoint twoDimObservation = new DoublePoint(point); 
		points.add(twoDimObservation);
	}
	
	private void printStats(List<CentroidCluster<Clusterable>> clusters) {
		System.out.println("Number of clusters: " + clusters.size());
		
		int i = 1;
		for (CentroidCluster<Clusterable> centroidCluster : clusters) {
			System.out.println("Center of cluster for cluster no. " + i + " is " + centroidCluster.getCenter() +
					" and it has " + centroidCluster.getPoints().size() + " points.");
			i++;
		}
		
		System.out.println("Score = " + sumOfClusterVariances.score(clusters));		
	}
	
	private void outputBubbleChartData(List<Clusterable> points, double[] centroid) {
		
		HashMap<Point_2d, Integer> pointWithIntensity = new HashMap<Point_2d, Integer>();
		
		for (Clusterable clusterable : points) {
			double x = clusterable.getPoint()[0];
			double y = clusterable.getPoint()[1];
			
			Point_2d currentPoint = new Point_2d(x, y);
			
			Integer existingPointCount = pointWithIntensity.get(currentPoint);
			
			if(existingPointCount == null) {
				pointWithIntensity.put(currentPoint, 1);
			}else {
				pointWithIntensity.put(currentPoint, existingPointCount + 1);
			}
		}
		
		int [] x_axis_data = new int[pointWithIntensity.keySet().size()]; 
		int [] y_axis_data = new int[pointWithIntensity.keySet().size()];
		double [] z_data = new double[pointWithIntensity.keySet().size()];
		
		int idx = 0;
		
		Iterator<Point_2d> points_2dIter = pointWithIntensity.keySet().iterator();
		while(points_2dIter.hasNext()) {
			Point_2d point_2d = points_2dIter.next();
			x_axis_data[idx] = (int)point_2d.getX();
			y_axis_data[idx] = (int)point_2d.getY();
			z_data[idx] = pointWithIntensity.get(point_2d);

			System.out.println(x_axis_data[idx] + "," + y_axis_data[idx] + "," + z_data[idx]);
			idx++;
		}
		System.out.println("Data for speed = " + centroid[0] + ", volume " + centroid[1]);

		System.out.println("*****Done*******");		
		
	}
}
