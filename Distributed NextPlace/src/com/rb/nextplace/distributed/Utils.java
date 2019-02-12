package com.rb.nextplace.distributed;

import com.google.common.collect.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.*;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.rb.nextplace.distributed.Location.Location2Tuple;

/**
 * Created by Rufat Babayev on 7/3/2017.
 */
public class Utils implements Serializable
{
    //generated serial version UID
    private static final long serialVersionUID = 6909337684543658828L;

    public static Map<Location, Double> distributedCumulativeGaussian(JavaSparkContext sc,
                                                                      int requiredPartitionCount,
                                                                      final Set<Location> uniqueLocations,
                                                                      double sigma) {
        final Broadcast<Set<Location>> broadcastUniqueLocations = sc.broadcast(uniqueLocations);

        JavaRDD<Location> uniqueLocationsRDD = sc.parallelize(new ArrayList<Location>(uniqueLocations), requiredPartitionCount);
        JavaPairRDD<Location, Double> locationCumGaussianPairRDD = uniqueLocationsRDD.mapToPair(
                new PairFunction<Location, Location, Double>() {
                    @Override
                    public Tuple2<Location, Double> call(Location muLocation) throws Exception {
                        double cumGaussian = 0;

                        //now for each unique location, increment cumGaussian
                        for (Location otherLocation : broadcastUniqueLocations.value()) {
                            cumGaussian += Utils.gaussian(muLocation, otherLocation, sigma);
                        } // for

                        return new Tuple2<Location, Double>(muLocation, cumGaussian); // / uniqueLocations.size());
                    } // call
                });

        //collect to avoid "lazy evaluation paradigm"
        Map<Location, Double> locationAndCumGaussianMap = locationCumGaussianPairRDD.collectAsMap();


        broadcastUniqueLocations.unpersist();
        uniqueLocationsRDD.unpersist();
        locationCumGaussianPairRDD.unpersist();
        uniqueLocationsRDD = null;
        locationCumGaussianPairRDD = null;

        return locationAndCumGaussianMap;
    } // distributedCumulativeGaussian


    public static Map<Location, Double> distributedWeightedCumulativeGaussian(JavaSparkContext sc,
                                                                              int requiredPartitionCount,
                                                                              final Set<Location> uniqueLocations,
                                                                              //final List<Location> visitLocations,
                                                                              final Map<Location, Long> locationAndTotalResSeconds_l_w_l_Map,
                                                                              final long totalResSecondsOfAllVisits_W,
                                                                              double sigma) {
        JavaRDD<Location> uniqueLocationsRDD = sc.parallelize(new ArrayList<Location>(uniqueLocations), requiredPartitionCount);

        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Number of unique locations: " + uniqueLocations.size());
        System.out.println("Required partition count: " + requiredPartitionCount);
        System.out.println("Partitions count of the created RDD: " + uniqueLocationsRDD.partitions().size() + "\n");


        //uniqueLocations is shared variable, therefore create a broadcast variable from it to broadcast it to nodes
        final Broadcast<Set<Location>> broadcastUniqueLocations = sc.broadcast(uniqueLocations);

        //broadcast visit locations
        //final Broadcast<List<Location>> broadcastVisitLocations = sc.broadcast(visitLocations);

        //broadcast sigma
        final Broadcast<Double> broadcastSigma = sc.broadcast(sigma);

        //broadcast the map and total res seconds
        final Broadcast<Map<Location, Long>> broadcastLocationAndTotalResSeconds_l_w_l_Map
                = sc.broadcast(locationAndTotalResSeconds_l_w_l_Map);
        final Broadcast<Long> broadcastTotalResSecondsOfAllVisits_W = sc.broadcast(totalResSecondsOfAllVisits_W);


        JavaPairRDD<Location, Double> locationWeightedCumGaussianPairRDD = uniqueLocationsRDD.mapToPair(
                new PairFunction<Location, Location, Double>() {
                    private static final long serialVersionUID = 5114520673310964130L;

                    @Override
                    public Tuple2<Location, Double> call(Location muLocation) throws Exception {
                        double weightedCumGaussian_F_mu_l_hat = 0;

                        //calculate F_mu_l_hat = sum{ol from visitLocations /*uniqueLocations*/} F_mu_l(ol) * w_ol / W
                        //instead we do F_mu_l_hat calculation with respect to visit locations which is the same as number of visits
                        for (Location otherLocation : /*broadcastVisitLocations.value())*/ broadcastUniqueLocations.value()) {
                            // weight is represented by visits' durations to a location,
                            // for the same location visitLocations, you will add the same weight more than once
                            // multiplicity of locations are represented by a weight, then it is questionable to add the same weight twice or more
                            long totalResSecondsOfOtherLocation_w_ol
                                    = broadcastLocationAndTotalResSeconds_l_w_l_Map.value().get(otherLocation);

                            //sigma is chosen as 10 meters based on average GPS accuracy
                            double gaussian_F_mu_l_ol = Utils.gaussian(muLocation, otherLocation, broadcastSigma.value()); //F_mu_l(ol)
                            weightedCumGaussian_F_mu_l_hat += gaussian_F_mu_l_ol
                                    // multiplication by weight
                                    * totalResSecondsOfOtherLocation_w_ol
                                    / (1.0 * broadcastTotalResSecondsOfAllVisits_W.value());
                        } // for

                        return new Tuple2<Location, Double>(muLocation, weightedCumGaussian_F_mu_l_hat);
                    } // call
                });

        //collect to avoid "lazy evaluation paradigm"
        Map<Location, Double> locationAndWeightedCumGaussianMap = locationWeightedCumGaussianPairRDD.collectAsMap();


        //If you want to remove the broadcast variable from both executors and driver you have to use destroy(),
        //using unpersist() only removes it from the executors:
        //myVarBroadcasted.destroy()
        //This method is blocking.
        broadcastUniqueLocations.unpersist();
        //broadcastVisitLocations.unpersist();
        broadcastLocationAndTotalResSeconds_l_w_l_Map.unpersist();
        broadcastTotalResSecondsOfAllVisits_W.unpersist();
        broadcastSigma.unpersist();
        uniqueLocationsRDD.unpersist();
        locationWeightedCumGaussianPairRDD.unpersist();
        uniqueLocationsRDD = null;
        locationWeightedCumGaussianPairRDD = null;

        return locationAndWeightedCumGaussianMap;
    } // distributedWeightedCumulativeGaussian


    //This method is not applicable in our case
//    //overloaded version with W_clusterID which it the total number of residence seconds of all location in cluster with clusterID
//    public static Map<Location, Double> distributedWeightedCumulativeGaussian(JavaSparkContext sc,
//                                                                              int requiredPartitionCount,
//                                                                              final Set<Location> uniqueLocations,
//                                                                              final Map<Location, Long> locationAndTotalResSeconds_l_w_l_Map,
//                                                                              final Map<Integer, HashSet<Location>> clusterMap,
//                                                                              final Map<Location, Integer> locationClusterIDMap,
//                                                                              double sigma)
//    {
//        HashMap<Integer, Long> clusterID_WClusterID_Map = new HashMap<>();
//        //populate clusterID_WClusterID_Map
//        for(Integer clusterID : clusterMap.keySet())
//        {
//            HashSet<Location> cluster = clusterMap.get(clusterID);
//            long W_clusterID = 0;
//            for(Location loc : cluster)
//            {
//                //w_loc_total_res_seconds is the total residence seconds of all visits performed to this loc
//                long w_loc_total_res_seconds = locationAndTotalResSeconds_l_w_l_Map.get(loc);
//                //therefore W_clusterID will be sum of w_loc of all locations inside it
//                W_clusterID += w_loc_total_res_seconds;
//            } // for
//
//            //update the map
//            clusterID_WClusterID_Map.put(clusterID, W_clusterID);
//        } // for
//
//
//        JavaRDD<Location> uniqueLocationsRDD = sc.parallelize(new ArrayList<Location>(uniqueLocations), requiredPartitionCount);
//
//        //uniqueLocations is shared variable, therefore create a broadcast variable from it to broadcast it to nodes
//        final Broadcast<Set<Location>> broadcastUniqueLocations = sc.broadcast(uniqueLocations);
//
//        //broadcast the locations and their total residence seconds
//        final Broadcast<Map<Location, Long>>  broadcastLocationAndTotalResSeconds_l_w_l_Map
//                = sc.broadcast(locationAndTotalResSeconds_l_w_l_Map);
//
//        //broadcast locations and their cluster ids
//        final Broadcast<Map<Location, Integer>> broadcastLocationClusterIDMap = sc.broadcast(locationClusterIDMap);
//
//        //broadcast cluster ids and their W_clusterIDs
//        final Broadcast<Map<Integer, Long>> broadcastClusterID_WClusterID_Map = sc.broadcast(clusterID_WClusterID_Map);
//
//
//        JavaPairRDD<Location, Double> locationWeightedCumGaussianPairRDD = uniqueLocationsRDD.mapToPair(
//                new PairFunction<Location, Location, Double>()
//                {
//                    private static final long serialVersionUID = 8064075486526426061L;
//
//                    @Override
//                    public Tuple2<Location, Double> call(Location muLocation) throws Exception
//                    {
//                        double weightedCumGaussian_F_mu_l_hat = 0;
//
//                        //calculate F_mu_l_hat = sum{ol from uniqueLocations} F_mu_l(ol) * w_ol / W
//                        for (Location otherLocation : broadcastUniqueLocations.value())
//                        {
//                            Integer clusterID_of_ol = broadcastLocationClusterIDMap.value().get(otherLocation);
//                            long W_ol_clusterID = broadcastClusterID_WClusterID_Map.value().get(clusterID_of_ol);
//
//                            long totalResSecondsOfOtherLocation_w_ol
//                                    = broadcastLocationAndTotalResSeconds_l_w_l_Map.value().get(otherLocation);
//
//                            double gaussian_F_mu_l_ol = Utils.gaussian(muLocation, otherLocation, 10); //F_mu_l(ol)
//                            weightedCumGaussian_F_mu_l_hat += gaussian_F_mu_l_ol
//                                    // multiplication by weight
//                                    * totalResSecondsOfOtherLocation_w_ol
//                                    / (1.0 * W_ol_clusterID);
//                        } // for
//
//                        return new Tuple2<Location, Double>(muLocation, weightedCumGaussian_F_mu_l_hat);
//                    } // call
//                });
//
//        //collect to avoid "lazy evaluation paradigm"
//        Map<Location, Double> locationAndWeightedCumGaussianMap = locationWeightedCumGaussianPairRDD.collectAsMap();
//
//
//        //If you want to remove the broadcast variable from both executors and driver you have to use destroy(),
//        //using unpersist() only removes it from the executors:
//        //myVarBroadcasted.destroy()
//        //This method is blocking.
//        broadcastUniqueLocations.unpersist();
//        broadcastLocationAndTotalResSeconds_l_w_l_Map.unpersist();
//        broadcastLocationClusterIDMap.unpersist();
//        broadcastClusterID_WClusterID_Map.unpersist();
//        uniqueLocationsRDD.unpersist();
//        locationWeightedCumGaussianPairRDD.unpersist();
//        uniqueLocationsRDD = null;
//        locationWeightedCumGaussianPairRDD = null;
//
//        return locationAndWeightedCumGaussianMap;
//    }


    // very slow
//    public static Map<Location, Double> distributedMutualGaussian(JavaSparkContext sc,
//                                                                  int requiredPartitionCount,
//                                                                  final Set<Location> uniqueLocations,
//                                                                  final Map<Location, Long> locationAndTotalResSeconds_l_w_l_Map,
//                                                                  final long totalResSecondsOfAllVisits_W,
//                                                                  double sigma) {
//        final ArrayList<Location> ul = new ArrayList<Location>(uniqueLocations);
//
//
//        final Broadcast<ArrayList<Location>> broadcastUl = sc.broadcast(ul);
//        //broadcast the map and total res seconds
//        final Broadcast<Map<Location, Long>> broadcastLocationAndTotalResSeconds_l_w_l_Map
//                = sc.broadcast(locationAndTotalResSeconds_l_w_l_Map);
//        final Broadcast<Long> broadcastTotalResSecondsOfAllVisits_W = sc.broadcast(totalResSecondsOfAllVisits_W);
//
//
//        //All operations preserve the order, except those that explicitly do not. Ordering is always "meaningful", not just after a sortBy. For example, if you read a file (sc.textFile) the lines of the RDD will be in the order that they were in the file.
//        //Without trying to give a complete list, map, filter, flatMap, and coalesce (with shuffle=false) do preserve the order. sortBy, partitionBy, join do not preserve shapeless.the order.
//        //The reason is that most RDD operations work on Iterators inside the partitions. So map or filter just has no way to mess up the order.
//        JavaRDD<Location> ulRDD = sc.parallelize(ul, requiredPartitionCount);
//
//        //zipWithIndex() preserves the order
//        JavaPairRDD<Location, Long> withIndex = ulRDD.zipWithIndex();
//        JavaPairRDD<Long, Location> indexKey = withIndex.mapToPair(new PairFunction<Tuple2<Location, Long>, Long, Location>() {
//            private static final long serialVersionUID = 217998430882193245L;
//
//            @Override
//            public Tuple2<Long, Location> call(Tuple2<Location, Long> locationLongTuple2) throws Exception {
//                return new Tuple2<Long, Location>(locationLongTuple2._2, locationLongTuple2._1);
//            }
//        }).cache();
//
//        //JavaPairRDD<Long, Location> indexSortedByKey = indexKey.sortByKey(true, requiredPartitionCount);
//
//        JavaPairRDD<Location2Tuple, Double> mutualGaussianRDD = indexKey.flatMapToPair(
//                new PairFlatMapFunction<Tuple2<Long, Location>, Location2Tuple, Double>() {
//                    private static final long serialVersionUID = 4055136931237064013L;
//
//                    @Override
//                    public Iterator<Tuple2<Location2Tuple, Double>> /*Iterable<Tuple2<Location2Tuple, Double>>*/ call(Tuple2<Long, Location> longLocationTuple2) throws Exception {
//                        Location muLocation = longLocationTuple2._2;
//                        long startIndex = longLocationTuple2._1; // mutual gaussian is calculated between muLocation and muLocation, too
//
//                        //list to hold Tuple2<Location.Location2Tuple, Double>
//                        ArrayList<Tuple2<Location2Tuple, Double>> list = new ArrayList<>();
//
//                        for (long index = startIndex; index < broadcastUl.value().size(); index++) {
//                            Location otherLocation = broadcastUl.value().get((int) index);
//
//                            double mutualGaussian = Utils.gaussian(muLocation,
//                                    otherLocation,
//                                    sigma);
//
//                            Location2Tuple location2Tuple = new Location2Tuple(muLocation, otherLocation);
//
//                            Tuple2<Location2Tuple, Double> location2TupleDouble = new Tuple2<Location2Tuple, Double>(location2Tuple, mutualGaussian);
//                            list.add(location2TupleDouble);
//                        } // for
//
//                        return list.iterator();
//                    }
//                });
//
//
//        Broadcast<Map<Location2Tuple, Double>> broadcastMutualGaussianMap = sc.broadcast(mutualGaussianRDD.collectAsMap());
//
//        JavaPairRDD<Location, Double> finalRDD = indexKey.mapToPair(new PairFunction<Tuple2<Long, Location>, Location, Double>() {
//            private static final long serialVersionUID = 1508522762374170175L;
//
//            @Override
//            public Tuple2<Location, Double> call(Tuple2<Long, Location> longLocationTuple2) throws Exception {
//                Location muLocation = longLocationTuple2._2;
//                double weightedCumGaussian_F_mu_l_hat = 0;
//
//                //calculate F_mu_l_hat = sum{ol from uniqueLocations} F_mu_l(ol) * w_ol / W
//                for (Location otherLocation : ul) {
//                    long totalResSecondsOfOtherLocation_w_ol
//                            = broadcastLocationAndTotalResSeconds_l_w_l_Map.value().get(otherLocation);
//
//                    Location2Tuple location2Tuple = new Location2Tuple(muLocation, otherLocation);
//
//                    double gaussian_F_mu_l_ol = broadcastMutualGaussianMap.value().get(location2Tuple); //F_mu_l(ol)
//                    weightedCumGaussian_F_mu_l_hat += gaussian_F_mu_l_ol
//                            // multiplication by weight
//                            * totalResSecondsOfOtherLocation_w_ol
//                            / (1.0 * broadcastTotalResSecondsOfAllVisits_W.value()); // weighting disabled
//                } // for
//
//                return new Tuple2<Location, Double>(muLocation, weightedCumGaussian_F_mu_l_hat);
//            }
//        });
//
//
//        //collectAsMap() internally creates a hash map
//        Map<Location, Double> finalMap = finalRDD.collectAsMap();
//
//        broadcastUl.unpersist();
//        broadcastLocationAndTotalResSeconds_l_w_l_Map.unpersist();
//        broadcastTotalResSecondsOfAllVisits_W.unpersist();
//        broadcastMutualGaussianMap.unpersist();
//        ulRDD.unpersist();
//        ulRDD = null;
//        withIndex.unpersist();
//        withIndex = null;
//        indexKey.unpersist();
//        indexKey = null;
//        mutualGaussianRDD.unpersist();
//        mutualGaussianRDD = null;
//        finalRDD.unpersist();
//        finalRDD = null;
//
//        return finalMap;
//    } // distributedMutualGaussian


    public static Map<Location, Double> distributedPeekOfGaussian(JavaSparkContext sc, Set<Location> uniqueLocations,
                                                                  int requiredPartitionCount, Tuple2<Double, Double> minLatitudeLongitudeTuple,
                                                                  Tuple2<Double, Double> maxLatitudeLongitudeTuple) {
        JavaRDD<Location> uniqueLocationsRDD = sc.parallelize(new ArrayList<Location>(uniqueLocations), requiredPartitionCount);
        JavaPairRDD<Location, Double> locationAndPeekPairRDD = uniqueLocationsRDD.mapToPair(new PairFunction<Location, Location, Double>() {
            @Override
            public Tuple2<Location, Double> call(Location location) throws Exception {
                Double peek = Utils.peekOfGaussian(location, minLatitudeLongitudeTuple._1, minLatitudeLongitudeTuple._2, //Location.MIN_LATITUDE, Location.MIN_LONGITUDE,
                        maxLatitudeLongitudeTuple._1, maxLatitudeLongitudeTuple._2, //Location.MAX_LATITUDE, Location.MAX_LONGITUDE,
                        10);

                return new Tuple2<Location, Double>(location, peek);
            } // call
        });

        //collect to avoid "lazy evaluation paradigm"
        Map<Location, Double> locationAndPeekMap = locationAndPeekPairRDD.collectAsMap();

        uniqueLocationsRDD.unpersist();
        locationAndPeekPairRDD.unpersist();
        uniqueLocationsRDD = null;
        locationAndPeekPairRDD = null;

        return locationAndPeekMap;
    } // distributedPeekOfGaussian


    public static Map<Location, Double> distributedWeightedGaussianKDE(JavaSparkContext sc,
                                                                       ArrayList<Visit> mergedVisits,
                                                                       boolean isCoordinateLevel) //,
    //long totalResidenceSecondsOfAllVisits,
    //int requiredPartitionCount)
    {
        ArrayList<Location> locationsOfMergedVisits = new ArrayList<Location>();
        ArrayList<Double> weightsOfLocationsOfMergedVisits = new ArrayList<Double>();
        HashSet<Location> uniqueLocations = new HashSet<Location>();

        //generate locations and weights for merged visits in the same order
        for (Visit v : mergedVisits) {
            uniqueLocations.add(v.getLocation());

            locationsOfMergedVisits.add(v.getLocation());
            weightsOfLocationsOfMergedVisits
                    .add((double) v.getTimeSeriesInstant().getResidenceTimeInSeconds());
            /// (totalResidenceSecondsOfAllVisits * 1.0) ); //  division by total res seconds disabled,
                                                            //  sum of weights will not be 1
        } // for


        System.out.println("Number of merged visits = " + mergedVisits.size());
        System.out.println("Number of unique locations = " + uniqueLocations.size());


        //broadcast variables for locationsOfMergedVisits and weightsOfLocationsOfMergedVisits
        final Broadcast<ArrayList<Location>> broadcastLocationsOfMergedVisits = sc.broadcast(locationsOfMergedVisits);
        final Broadcast<ArrayList<Double>> broadcastWeightsOfLocationsOfMergedVisits = sc.broadcast(weightsOfLocationsOfMergedVisits);


        JavaRDD<Location> ulRDD = sc.parallelize(new ArrayList<Location>(uniqueLocations), uniqueLocations.size()); //requiredPartitionCount);


        JavaPairRDD<Location, Double> uniqueLocationAndWeightedGaussianKDERDD = ulRDD.mapToPair(new PairFunction<Location, Location, Double>() {
            @Override
            public Tuple2<Location, Double> call(Location location) throws Exception {
                double weightedGaussianKDE
                        = Utils.weightedGaussianKDE(location, broadcastLocationsOfMergedVisits.value(),
                        broadcastWeightsOfLocationsOfMergedVisits.value(),
                        10);

                return new Tuple2<Location, Double>(location, weightedGaussianKDE);
            } // call
        });


        //collect to avoid "lazy evaluation paradigm"
        Map<Location, Double> uniqueLocationAndWeightedGaussianKDEMap
                = uniqueLocationAndWeightedGaussianKDERDD.collectAsMap();


        broadcastLocationsOfMergedVisits.unpersist();
        broadcastWeightsOfLocationsOfMergedVisits.unpersist();
        ulRDD.unpersist();
        uniqueLocationAndWeightedGaussianKDERDD.unpersist();
        ulRDD = null;
        uniqueLocationAndWeightedGaussianKDERDD = null;


        return uniqueLocationAndWeightedGaussianKDEMap;
    } // distributedWeightedGaussianKDE


    //helper method to compute thresholdT -> AvgSignLocPerUser and thresholdT -> AvgSignTimePercentPerUser
    public static Map<Double, Tuple2<Double, Double>> distributedGPSAvgSignLocAvgSignTime(JavaSparkContext sc,
                                                                                          Dataset dataset,
                                                                                          String localPathString,
                                                                                          boolean listLocalFilesRecursively,
                                                                                          double epsilon,
                                                                                          int minPts,
                                                                                          ArrayList<Double> thresholds) {
        List<String> ppUserFilePaths
                = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        if (dataset == Dataset.CenceMeGPS) {
            //sort the file names such that CenceMeLiteLog1 comes before CenceMeLiteLog10
            ppUserFilePaths.sort(new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    String[] s1Split = s1.split("Log");
                    String[] s2Split = s2.split("Log");
                    return Integer.valueOf(s1Split[1].split("_")[0])
                            .compareTo(Integer.valueOf(s2Split[1].split("_")[0]));
                } // compare
            });
        } // if

        return distributedGPSAvgSignLocAvgSignTime(sc, dataset, ppUserFilePaths, epsilon, minPts, thresholds);
    } //

    //helper method to compute thresholdT -> AvgSignLocPerUser and thresholdT -> AvgSignTimePercentPerUser
    public static Map<Double, Tuple2<Double, Double>> distributedGPSAvgSignLocAvgSignTime(JavaSparkContext sc,
                                                                                          Dataset dataset,
                                                                                          List<String> ppUserFilePaths,
                                                                                          //boolean listLocalFilesRecursively,
                                                                                          double epsilon,
                                                                                          int minPts,
                                                                                          ArrayList<Double> thresholds
                                                                                          //, String ... deserializationPaths
    ) {
        //distance measure for calculating haversine distance between locations during clustering
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };


        //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap = new HashMap<>();
        //for(String deserializationPath : deserializationPaths)
        //{
        //    //noinspection unchecked
        //    userFileNameLocationWeightedCumulativeGaussianMap.putAll
        //            ( (HashMap<String, Map<Location, Double>>)
        //                    deserialize(deserializationPath) );
        //} // for
        //System.out.println(userFileNameLocationWeightedCumulativeGaussianMap.keySet().size());

        //List<String> ppUserFilePaths
        //        = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        //if (dataset == Dataset.CenceMeGPS) {
        //    //sort the file names such that CenceMeLiteLog1 comes before CenceMeLiteLog10
        //    ppUserFilePaths.sort(new Comparator<String>() {
        //        @Override
        //        public int compare(String s1, String s2) {
        //            String[] s1Split = s1.split("Log");
        //            String[] s2Split = s2.split("Log");
        //            return Integer.valueOf(s1Split[1].split("_")[0])
        //                    .compareTo(Integer.valueOf(s2Split[1].split("_")[0]));
        //        } // compare
        //    });
        //} // if

        //user name -> merged visits list map
        //HashMap<String, List<Visit>> userFileNameMergedVisitsMap = new HashMap<>();

        //user name -> unique gps points map
        //HashMap<String, HashSet<Location>> userFileNameUniqueGPSPointsMap = new HashMap<>();

        //user name -> total residence second of visit of this user map
        //HashMap<String, Long> userFileNameTotalResSecondsOfAllVisitsMap = new HashMap<>();

        //for each user, update a user -> merged visits map and user -> unique gps points map
        //for(String ppUserFilePath: ppUserFilePaths)
        //{
        //    String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
        //
        //    String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
        //    ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);
        //    userFileNameMergedVisitsMap.put(keyFileName, mergedVisits);
        //
        //    //unique GPS points of this user
        //    //HashSet<Location> uniqueGPSPointsOfThisUser = new HashSet<>();
        //    //total residence seconds of this user
        //    long totalResSecondsOfAllVisitsOfThisUser = 0;
        //
        //    //for each visit update both variables
        //    for(Visit v : mergedVisits)
        //    {
        //        //uniqueGPSPointsOfThisUser.add(v.getLocation());
        //        totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
        //    } // for
        //
        //    //userFileNameUniqueGPSPointsMap.put(keyFileName, uniqueGPSPointsOfThisUser);
        //    userFileNameTotalResSecondsOfAllVisitsMap.put(keyFileName, totalResSecondsOfAllVisitsOfThisUser);
        //} // for


        //thresholdT -> avg sign loc and thresholdT -> avg sign time map
        //LinkedHashMap<Double, Double> thresholdTAvgSignLocPerUserMap = new LinkedHashMap<>();
        //LinkedHashMap<Double, Double> thresholdTSignTimePercentPerUserMap = new LinkedHashMap<>();

        //count of all significant locations that each user visited
        LongAccumulator countOfAllSignificantLocationsOfUsers = new LongAccumulator();
        sc.sc().register(countOfAllSignificantLocationsOfUsers);

        //total number of visits
        //long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);

        //total residence seconds before pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);

        //double average proportion of time spent by each user in significant places
        DoubleAccumulator sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces = new DoubleAccumulator();
        sc.sc().register(sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces);

        //total number of outlier gps points of users
        LongAccumulator totalOutlierGPSPoints = new LongAccumulator();
        sc.sc().register(totalOutlierGPSPoints);

        //broadcast all necessary variables
        Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);
        Broadcast<Double> broadcastEpsilon = sc.broadcast(epsilon);
        Broadcast<Integer> broadcastMinPts = sc.broadcast(minPts);
        Broadcast<DistanceMeasure> broadcastDM = sc.broadcast(dm);

        //cache rdd
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, ppUserFilePaths.size()).cache();

        //resulting thresholdT -> (avg sign loc, avg sign time) map
        LinkedHashMap<Double, Tuple2<Double, Double>> resultsMap = new LinkedHashMap<>();

        //for each threshold, extract avg sign loc and avg sign time
        for (Double thresholdT : thresholds) {
            System.out.println("CHOSEN THRESHOLD: " + thresholdT);
            //Tuple2<Double, Tuple2<Double, Double>> thresholdTAvgSignLocAvgSignTimePercentPerUserTuple2
            //= distributedExtractGPSAvgSignLocAvgSignTime(sc, dataset, ppUserFilePaths,
            //                                            thresholdT, epsilon, minPts, dm);


            //broadcast variable for threshold T
            Broadcast<Double> broadcastThresholdT = sc.broadcast(thresholdT);


            //for each pp user file path, perform avg sign loc and avg sign time
            ppUserFilePathsRDD.foreach(new VoidFunction<String>() {
                private static final long serialVersionUID = 3466652089392547361L;

                @Override
                public void call(String ppUserFilePath) throws Exception {
                    //file name is the key for all passed map arguments
                    String keyFileName = Utils.fileNameFromPath(ppUserFilePath);

                    //obtain merged visits from the map
                    List<Visit> mergedVisits = correspondingMergedVisits(broadcastDataset.value(), keyFileName, false);

                    //total residence seconds of all visits of this user
                    long totalResSecondsOfAllVisitsOfThisUser = 0;

                    //total residence seconds of significant visits (visits performed to significant locations) of this user
                    long totalResSecondsOfSignificantVisitsOfThisUser = 0;

                    //obtain \hat_{F_\mu} of each user
                    Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                            = correspondingTuple(broadcastDataset.value(), keyFileName, false);


                    //above threshold GPS points
                    ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();

                    //for each visit's location, if location weighted cumulative Gaussian value is above the threshold,
                    //add it to the list
                    for (Visit v : mergedVisits) {
                        totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                        Location gpsPoint = v.getLocation();
                        double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                        //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                        if (aggregatedWeightedGaussian > broadcastThresholdT.value()
                                || Double.compare(aggregatedWeightedGaussian, broadcastThresholdT.value()) == 0) {
                            aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                        } // if
                    } // for
                    gpsPointAndWeightedCumulativeGaussianMap.clear();
                    gpsPointAndWeightedCumulativeGaussianMap = null;


                    //update total residence seconds of all visits of all users
                    totalResSecondsOfAllVisitsOfUsersBeforePreprocessing.add(totalResSecondsOfAllVisitsOfThisUser);


                    //CLUSTERING START...
                    DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(broadcastEpsilon.value(),
                            broadcastMinPts.value(), broadcastDM.value());

                    //cluster aboveThresholdGPSPointsOfThisUser
                    List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


                    //clear memory
                    int aboveThresholdGPSPointsOfThisUserSize = aboveThresholdGPSPointsOfThisUser.size();
                    aboveThresholdGPSPointsOfThisUser.clear();
                    aboveThresholdGPSPointsOfThisUser = null;


                    //remove the clusters with insufficient number of elements
                    clusters.removeIf(locationCluster -> locationCluster.getPoints().size() < broadcastMinPts.value() + 1);

                    //update sum of count of all significant locations of each user
                    countOfAllSignificantLocationsOfUsers.add(clusters.size());

                    //total number of clustered gps points
                    int totalGPSPointsInClusters = 0;

                    //map to hold gps point and its cluster id
                    HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

                    //lastClusterID variable for each obtained cluster
                    int lastClusterID = 1;

                    //obtain iterator for clusters
                    Iterator<Cluster<Location>> clusterIterator = clusters.iterator();

                    //now for each cluster update list and mp
                    while (clusterIterator.hasNext()) {
                        List<Location> gpsPointsInThisCluster = clusterIterator.next().getPoints();
                        totalGPSPointsInClusters += gpsPointsInThisCluster.size();


                        //now populate location and cluster id map
                        for (Location gpsPoint : gpsPointsInThisCluster) {
                            //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                            gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                        } // for


                        //remove the cluster
                        clusterIterator.remove();

                        //increment clusterID for next cluster
                        lastClusterID++;
                    } // for
                    clusters = null;

                    //update the total outlier GPS points
                    totalOutlierGPSPoints.add(aboveThresholdGPSPointsOfThisUserSize - totalGPSPointsInClusters);
                    //CLUSTERING END...

                    //get the iterator for merged visits
                    Iterator<Visit> mergedVisitsIterator = mergedVisits.iterator();

                    //for each visit, if visit is significant, then reroute it its corresponding significant location
                    while (mergedVisitsIterator.hasNext()) {
                        //obtain the visit
                        Visit v = mergedVisitsIterator.next();

                        //obtain the location if this visit
                        Location location = v.getLocation();

                        //check whether this visit's location is within aboveThresholdGPSPoints,
                        //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                        if (gpsPointClusterIDMap.containsKey(location)) {
                            long significantVisitResidenceSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                            //update total residence seconds of significant visits
                            totalResSecondsOfSignificantVisitsOfThisUser
                                    += significantVisitResidenceSeconds;


                            //update total residence seconds of all visits of all users after preprocessing
                            totalResSecondsOfAllVisitsOfUsersAfterPreprocessing.add(significantVisitResidenceSeconds);
                        } // if


                        //remove the processed visit from the list;
                        //in any case, it should be removed, either its location is contained or not
                        mergedVisitsIterator.remove();

                    } // for
                    mergedVisits = null;
                    gpsPointClusterIDMap.clear();
                    gpsPointClusterIDMap = null;


                    //update total proportion of time spent by users by a proportion of this user;
                    //at the end of for loop we will divide this number by number of users
                    double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
                    sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces
                            .add(Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser);
                } // call
            });


            //print threshold T and num outliers
            System.out.println("(" + thresholdT + "," + totalOutlierGPSPoints.value() + ")"); //+ "," + totalOutlierClusters);


            Tuple2<Double, Tuple2<Double, Double>> thresholdTAvgSignLocAvgSignTimePercentPerUserTuple2
                    = new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
                    countOfAllSignificantLocationsOfUsers.value() / (double) ppUserFilePaths.size(),
                    //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),

                    sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces.value() / ppUserFilePaths.size()
                    //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
            ));


            //unpersist threshold T; new iteration will create new broadcast variable
            broadcastThresholdT.unpersist();
            //reset accumulators, new iteration will update the accumulator
            totalResSecondsOfAllVisitsOfUsersBeforePreprocessing.reset();
            totalResSecondsOfAllVisitsOfUsersAfterPreprocessing.reset();
            totalOutlierGPSPoints.reset();
            countOfAllSignificantLocationsOfUsers.reset();
            sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces.reset();


            Double threshold = thresholdTAvgSignLocAvgSignTimePercentPerUserTuple2._1;
            Tuple2<Double, Double> avgSignLocAvgSignTimeTuple = thresholdTAvgSignLocAvgSignTimePercentPerUserTuple2._2;
            resultsMap.put(threshold, avgSignLocAvgSignTimeTuple);
        } // for


        //unpersist rdds
        ppUserFilePathsRDD.unpersist();
        ppUserFilePathsRDD = null;
        broadcastDataset.unpersist();
        broadcastDM.unpersist();
        broadcastEpsilon.unpersist();
        broadcastMinPts.unpersist();


        /*
        System.out.println("\n\nthresholdT -> AvgSignLocPerUser:");
        for(Double thresholdT : thresholdTAvgSignLocPerUserMap.keySet())
        {
            System.out.println(thresholdT + ", " + thresholdTAvgSignLocPerUserMap.get(thresholdT));
        } // for


        System.out.println();

        for(Double thresholdT : thresholdTAvgSignLocPerUserMap.keySet())
        {
            System.out.print(thresholdT + ", ");
        }

        System.out.println();

        for(Double thresholdT : thresholdTAvgSignLocPerUserMap.keySet())
        {
            System.out.print(thresholdTAvgSignLocPerUserMap.get(thresholdT) + ", ");
        }
        System.out.println();




        System.out.println("\n\nthresholdT -> AvgSignTimePercentPerUser:");
        for(Double thresholdT : thresholdTSignTimePercentPerUserMap.keySet())
        {
            System.out.println(thresholdT + ", " + thresholdTSignTimePercentPerUserMap.get(thresholdT));
        }

        System.out.println("");

        for(Double thresholdT : thresholdTSignTimePercentPerUserMap.keySet())
        {
            System.out.print(thresholdT + ", ");
        }

        System.out.println();

        for(Double thresholdT : thresholdTSignTimePercentPerUserMap.keySet())
        {
            System.out.print(thresholdTSignTimePercentPerUserMap.get(thresholdT) + ", ");
        }
        System.out.println("\n");
        */

        return resultsMap;
    } // distributedGPSAvgSignLocAvgSignTime


    //helper method to process find gps avg sign loc and sign time in chunks
    public static Map<Double, Triple<Double, Double, Double>> distributedGPSAvgSignLocAvgSignTimeInChunks(JavaSparkContext sc,
                                                                                                          Dataset dataset,
                                                                                                          String localPathString,
                                                                                                          boolean listLocalFilesRecursively,
                                                                                                          ArrayList<Double> thresholds,
                                                                                                          double epsilon,
                                                                                                          int minPoints
                                                                                                          //, String ... deserializationPaths
    ) {
        //distance measure for calculating haversine distance between locations during clustering
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };


        List<String> ppUserFilePaths
                = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);


        //broadcast all necessary variables
        Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);
        Broadcast<Double> broadcastEpsilon = sc.broadcast(epsilon);
        Broadcast<Integer> broadcastMinPoints = sc.broadcast(minPoints);
        Broadcast<DistanceMeasure> broadcastDistanceMeasure = sc.broadcast(dm);

        //adjust numSlices accordingly
        int numThresholds = thresholds.size();
        int numSlices = numThresholds > 300000 ? numThresholds / 1000 : (numThresholds > 100000 ? numThresholds / 100 :
                (numThresholds > 10000 ? numThresholds / 100 : (numThresholds > 3000 ? numThresholds / 10 : numThresholds)));

        //create an rdd from thresholds;
        //for CenceMeGPS, #thresholds is 749932, so numSlices becomes 749
        JavaRDD<Double> thresholdsRDD = sc.parallelize(thresholds, numSlices).cache(); // cache rdd to speed up performance


        LinkedHashMap<Integer, Integer> startIndexEndIndexMap = new LinkedHashMap<>();
        for (int start = 0; start < 520; start += 20) {
            startIndexEndIndexMap.put(start, start + 20);
        } // for
        startIndexEndIndexMap.put(520, 536);


//        LinkedHashMap<Integer, Integer> startIndexEndIndexMap = new LinkedHashMap<>();
//        startIndexEndIndexMap.put(0, 100);
//        startIndexEndIndexMap.put(100, 200);
//        startIndexEndIndexMap.put(200, 300);
//        startIndexEndIndexMap.put(300, 400);
//        startIndexEndIndexMap.put(400, 500);
//        startIndexEndIndexMap.put(500, 536);


        //start from 3, then will be updated to 4, 5, 6
        int fileAppendix = 1;

        //initial empty map
        //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap = new HashMap<>();

        ListMultimap<Double, Triple<Double, Double, Double>> thresholdAllSignLocSumOfProportionsMap = MultimapBuilder.treeKeys().arrayListValues().build();

        for (int startIndex : startIndexEndIndexMap.keySet()) {
            int endIndex = startIndexEndIndexMap.get(startIndex);

            //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap
            //        = userFileNameLocationWeightedCumulativeGaussianMap(sc, Dataset.Cabspotting,
            //        "raw/Cabspotting/data_all_loc",
            //        10.0, startIndex, endIndex, false);


            //serialize("raw/Cabspotting/serialization/userFileNameLocationWeightedCumulativeGaussianMap"
            //                + fileAppendix +  ".ser", userFileNameLocationWeightedCumulativeGaussianMap);


            ////noinspection unchecked
            //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap
            //            =
            ////userFileNameLocationWeightedCumulativeGaussianMap.putAll(
            //        (HashMap<String, Map<Location, Double>>)
            //                deserialize(deserializationPaths[fileAppendix - 1]);  //) ;

            //System.out.println(userFileNameLocationWeightedCumulativeGaussianMap);

            fileAppendix++;


            List<String> filteredPPUserFilePaths = new ArrayList<>();
            for (int start = startIndex; start < endIndex; start++) {
                filteredPPUserFilePaths.add(ppUserFilePaths.get(start));
            } // for


            //user name -> merged visits list map
            HashMap<String, List<Visit>> userFileNameMergedVisitsMap = new HashMap<>();

            //user name -> unique gps points map
            //HashMap<String, HashSet<Location>> userFileNameUniqueGPSPointsMap = new HashMap<>();

            //user name -> total residence second of visit of this user map
            HashMap<String, Long> userFileNameTotalResSecondsOfAllVisitsMap = new HashMap<>();

            //for each user, update a user -> merged visits map and user -> unique gps points map
            for (String filteredPPUserFilePath : filteredPPUserFilePaths) {
                String keyFileName = Utils.fileNameFromPath(filteredPPUserFilePath);

                String fileContents = Utils.fileContentsFromLocalFilePath(filteredPPUserFilePath);
                ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);
                userFileNameMergedVisitsMap.put(keyFileName, mergedVisits);

                //unique GPS points of this user
                //HashSet<Location> uniqueGPSPointsOfThisUser = new HashSet<>();
                //total residence seconds of this user
                long totalResSecondsOfAllVisitsOfThisUser = 0;

                //for each visit update both variables
                for (Visit v : mergedVisits) {
                    //uniqueGPSPointsOfThisUser.add(v.getLocation());
                    totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
                } // for

                //userFileNameUniqueGPSPointsMap.put(keyFileName, uniqueGPSPointsOfThisUser);
                userFileNameTotalResSecondsOfAllVisitsMap.put(keyFileName, totalResSecondsOfAllVisitsOfThisUser);
            } // for


            //broadcast all necessary variables
            //Broadcast<HashMap<String, Map<Location, Double>>> broadcastUserFileNameLocationWeightedCumulativeGaussianMap
            //        = sc.broadcast(userFileNameLocationWeightedCumulativeGaussianMap);
            Broadcast<List<String>> broadcastFilteredPPUserFilePaths = sc.broadcast(filteredPPUserFilePaths);

            Broadcast<HashMap<String, List<Visit>>> broadcastUserFileNameMergedVisitsMap = sc.broadcast(userFileNameMergedVisitsMap);
            //Broadcast<HashMap<String, HashSet<Location>>> broadcastUserFileNameUniqueGPSPointsMap
            //        = sc.broadcast(userFileNameUniqueGPSPointsMap);
            Broadcast<HashMap<String, Long>> broadcastUserFileNameTotalResSecondsOfAllVisitsMap
                    = sc.broadcast(userFileNameTotalResSecondsOfAllVisitsMap);


            //map thresholdsRDD to <Double, Tuple2<Double, Double>, where first part of the pair is threshold
            //and second part first element is AvgSignLoc, and second element is AvgSignTime
            JavaPairRDD<Double, Triple<Double, Double, Double>> resultsRDD = thresholdsRDD.mapToPair(
                    new PairFunction<Double, Double, Triple<Double, Double, Double>>() {
                        private static final long serialVersionUID = -2157170143888116539L;

                        @Override
                        public Tuple2<Double, Triple<Double, Double, Double>> call(Double thresholdT) throws Exception {
                            //System.out.println("CHOSEN THRESHOLD: " + thresholdT);
                            return Utils.extractGPSAvgSignLocAvgSignTime(
                                    broadcastDataset.value(),
                                    broadcastFilteredPPUserFilePaths.value(),
                                    thresholdT,
                                    broadcastUserFileNameMergedVisitsMap.value(),
                                    //broadcastUserFileNameUniqueGPSPointsMap.value(),
                                    broadcastUserFileNameTotalResSecondsOfAllVisitsMap.value(),
                                    //broadcastUserFileNameLocationWeightedCumulativeGaussianMap.value(),
                                    broadcastEpsilon.value(),
                                    broadcastMinPoints.value(),
                                    broadcastDistanceMeasure.value());


//                            Utils.distributedExtractGPSAvgSignLocAvgSignTime(broadcastDataset.value(),
//                                    broadcastFilteredPPUserFilePaths.value(),
//                                    thresholdT,
//                                    userFileNameLocationWeightedCumulativeGaussianMap,
//                                    broadcastEpsilon.value(),
//                                    broadcastMinPoints.value(),
//                                    broadcastDistanceMeasure.value());
                        } // call
                    });


            //collect the results
            SetMultimap<Double, Triple<Double, Double, Double>> resultMultiMap = Multimaps.forMap(resultsRDD.collectAsMap());
            serialize("raw/Cabspotting/data_all_loc/serialization3/result_" + startIndex + "_to_"
                    + endIndex + "_.ser", resultMultiMap);
            thresholdAllSignLocSumOfProportionsMap.putAll(resultMultiMap);


            resultsRDD.unpersist();
            resultsRDD = null;
            broadcastFilteredPPUserFilePaths.unpersist();
            //broadcastUserFileNameLocationWeightedCumulativeGaussianMap.unpersist();
            broadcastUserFileNameMergedVisitsMap.unpersist();
            //broadcastUserFileNameUniqueGPSPointsMap.unpersist();
            broadcastUserFileNameTotalResSecondsOfAllVisitsMap.unpersist();


            //clear all unnecessary maps
            //userFileNameLocationWeightedCumulativeGaussianMap.clear();
            //filteredPPUserFilePaths.clear();
            //userFileNameMergedVisitsMap.clear();;
            //userFileNameUniqueGPSPointsMap.clear();
            //userFileNameTotalResSecondsOfAllVisitsMap.clear();
        } // for


        //keys are already ordered in thresholdAllSignLocSumOfProportions multi map
        return collectResultsForThresholds(thresholdAllSignLocSumOfProportionsMap, ppUserFilePaths.size());
    } // distributedGPSAvgSignLocAvgSignTimeInChunks


    //helper method to collect the threshold based results from set-multimap
    private static Map<Double, Triple<Double, Double, Double>> collectResultsForThresholds(Multimap<Double, Triple<Double, Double, Double>> thresholdAllSignLocSumOfProportionsMap,
                                                                                           int consideredFileCountInTheDataset) {
        //keys are already ordered in thresholdAllSignLocSumOfProportions multi map
        LinkedHashMap<Double, Triple<Double, Double, Double>> finalMap = new LinkedHashMap<>();
        for (Double threshold : thresholdAllSignLocSumOfProportionsMap.keySet()) {
            double allSignLoc = 0;
            double sumOfProportions = 0;
            double sumOfOutlierGPSPoints = 0;
            for (Triple<Double, Double, Double> allSignLocSumOfProportionsOutlierGPSPointsTriple : thresholdAllSignLocSumOfProportionsMap.get(threshold)) {
                allSignLoc += allSignLocSumOfProportionsOutlierGPSPointsTriple.getLeft();
                sumOfProportions += allSignLocSumOfProportionsOutlierGPSPointsTriple.getMiddle();
                sumOfOutlierGPSPoints += allSignLocSumOfProportionsOutlierGPSPointsTriple.getRight();
            } // for
            finalMap.put(threshold, Triple.of(allSignLoc / consideredFileCountInTheDataset,
                    sumOfProportions / consideredFileCountInTheDataset, sumOfOutlierGPSPoints / consideredFileCountInTheDataset));
        } // for

        return finalMap;
    } // collectResultsForThresholds


    //helper method
    public static Map<Double, Triple<Double, Double, Double>> deserializeAndCollect(List<String> deserializationPaths, int consideredFileCountInTheDataset) {
        ListMultimap<Double, Triple<Double, Double, Double>> thresholdAllSignLocSumOfProportionsMap = MultimapBuilder.treeKeys().arrayListValues().build();

        //for each deserialization path, deserialize and collect
        for (String deserializationPath : deserializationPaths) {
            SetMultimap<Double, Triple<Double, Double, Double>> resultMultiMap = (SetMultimap<Double, Triple<Double, Double, Double>>) deserialize(deserializationPath, true);
            thresholdAllSignLocSumOfProportionsMap.putAll(resultMultiMap);
        } // for

        return collectResultsForThresholds(thresholdAllSignLocSumOfProportionsMap, consideredFileCountInTheDataset);
    } // deserializeAndCollect


    //helper method to obtain the first number of elements from array list
    public static <E> List<E> head(List<E> original, int numberOfElementsFromBeginning) {
        if (numberOfElementsFromBeginning <= 0)
            throw new IllegalArgumentException("number of elements cannot be smaller than or equal to 0");

        if (numberOfElementsFromBeginning > original.size()) {
            System.err.println("size of the list is smaller than " +
                    "the resuqested number of elements, setting number of elements to list size");
            numberOfElementsFromBeginning = original.size();
        }

        ArrayList<E> head = new ArrayList<>();
        for (int index = 0; index < numberOfElementsFromBeginning; index++)
            head.add(original.get(index));

        return head;
    } // head


    //helper method to compute thresholdT -> AvgSignLocPerUser and thresholdT -> AvgSignTimePercentPerUser
    //in a distributed fashion
    public static Map<Double, Triple<Double, Double, Double>> distributedGPSAvgSignLocAvgSignTime(JavaSparkContext sc,
                                                                                                  Dataset dataset,
                                                                                                  String localPathString,
                                                                                                  boolean listLocalFilesRecursively,
                                                                                                  ArrayList<Double> thresholds,
                                                                                                  double epsilon,
                                                                                                  int minPoints
                                                                                                  //, String ... deserializationPaths
    ) {
        //distance measure for calculating haversine distance between locations during clustering
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };


        //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap = new HashMap<>();
        //for(String deserializationPath : deserializationPaths)
        //{
        //    //noinspection unchecked
        //    userFileNameLocationWeightedCumulativeGaussianMap.putAll
        //            ( (HashMap<String, Map<Location, Double>>)
        //                    deserialize(deserializationPath) );
        //} // for
        //System.out.println(userFileNameLocationWeightedCumulativeGaussianMap.keySet().size());


        List<String> ppUserFilePaths
                = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        if (dataset == Dataset.CenceMeGPS) {
            //sort the file names such that CenceMeLiteLog1 comes before CenceMeLiteLog10
            ppUserFilePaths.sort(new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    String[] s1Split = s1.split("Log");
                    String[] s2Split = s2.split("Log");
                    return Integer.valueOf(s1Split[1].split("_")[0])
                            .compareTo(Integer.valueOf(s2Split[1].split("_")[0]));
                } // compare
            });
        } // if


        //ppUserFilePaths = Utils.sample(ppUserFilePaths, 5);


        //user name -> merged visits list map
        //HashMap<String, List<Visit>> userFileNameMergedVisitsMap = new HashMap<>();

        //user name -> unique gps points map
        //HashMap<String, HashSet<Location>> userFileNameUniqueGPSPointsMap = new HashMap<>();

        //user name -> total residence second of visit of this user map
        //HashMap<String, Long> userFileNameTotalResSecondsOfAllVisitsMap = new HashMap<>();

        //for each user, update a user -> merged visits map and user -> unique gps points map
        //for(String ppUserFilePath: ppUserFilePaths)
        //{
        //String keyFileName = Utils.fileNameFromPath(ppUserFilePath);

        //InputStream in = Utils.inputStreamFromLocalFilePath(ppUserFilePath);
        //ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileInputStream(in);
        //userFileNameMergedVisitsMap.put(keyFileName, mergedVisits);


        //if(dataset == Dataset.Cabspotting)
        //         Utils.serialize("raw/Cabspotting/data_all_loc/serialization7/mergedVisits"
        //                 + "_" + keyFileName + "_" +  ".ser", mergedVisits);
        //else if(dataset == Dataset.CenceMeGPS)
        //         Utils.serialize("raw/CenceMeGPS/data_all_loc/serialization5/mergedVisits"
        //                 + "_" + keyFileName + "_" +  ".ser", mergedVisits);


        //unique GPS points of this user
        //HashSet<Location> uniqueGPSPointsOfThisUser = new HashSet<>();
        //total residence seconds of this user
        //long totalResSecondsOfAllVisitsOfThisUser = 0;

        //for each visit update both variables
        //for(Visit v : mergedVisits)
        //{
        //uniqueGPSPointsOfThisUser.add(v.getLocation());
        //    totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
        //} // for

        //userFileNameUniqueGPSPointsMap.put(keyFileName, uniqueGPSPointsOfThisUser);
        //userFileNameTotalResSecondsOfAllVisitsMap.put(keyFileName, totalResSecondsOfAllVisitsOfThisUser);
        //} // for

        //System.exit(0);


        //broadcast all necessary variables
        Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);
        Broadcast<Double> broadcastEpsilon = sc.broadcast(epsilon);
        Broadcast<Integer> broadcastMinPoints = sc.broadcast(minPoints);
        Broadcast<DistanceMeasure> broadcastDistanceMeasure = sc.broadcast(dm);
        //Broadcast<HashMap<String, Map<Location, Double>>> broadcastUserFileNameLocationWeightedCumulativeGaussianMap
        //        = sc.broadcast(userFileNameLocationWeightedCumulativeGaussianMap);
        Broadcast<List<String>> broadcastPPUserFilePaths = sc.broadcast(ppUserFilePaths);

        //Broadcast<HashMap<String, List<Visit>>> broadcastUserFileNameMergedVisitsMap = sc.broadcast(userFileNameMergedVisitsMap);
        //Broadcast<HashMap<String, HashSet<Location>>> broadcastUserFileNameUniqueGPSPointsMap
        //        = sc.broadcast(userFileNameUniqueGPSPointsMap);
        //Broadcast<HashMap<String, Long>> broadcastUserFileNameTotalResSecondsOfAllVisitsMap
        //        = sc.broadcast(userFileNameTotalResSecondsOfAllVisitsMap);


        //adjust numSlices accordingly
        int numThresholds = thresholds.size();
        int numSlices = numThresholds > 300000 ? numThresholds / 1000 : (numThresholds > 100000 ? numThresholds / 100 :
                (numThresholds > 10000 ? numThresholds / 100 : (numThresholds > 3000 ? numThresholds / 10 : numThresholds)));

        //create an rdd from thresholds;
        //for CenceMeGPS, #thresholds is 749932, so numSlices becomes 749
        JavaRDD<Double> thresholdsRDD = sc.parallelize(thresholds, thresholds.size()); //numSlices);

        //map thresholdsRDD to <Double, Tuple2<Double, Double>, where first part of the pair is threshold
        //and second part first element is AvgSignLoc, and second element is AvgSignTime
        JavaPairRDD<Double, Triple<Double, Double, Double>> resultsRDD = thresholdsRDD.mapToPair(
                new PairFunction<Double, Double, Triple<Double, Double, Double>>() {
                    private static final long serialVersionUID = -2157170143888116539L;

                    @Override
                    public Tuple2<Double, Triple<Double, Double, Double>> call(Double thresholdT) throws Exception {
                        //System.out.println("CHOSEN THRESHOLD: " + thresholdT);
                        return Utils.extractGPSAvgSignLocAvgSignTime(
                                broadcastDataset.value(),
                                broadcastPPUserFilePaths.value(),
                                thresholdT,
                                null, //broadcastUserFileNameMergedVisitsMap.value(),
                                //broadcastUserFileNameUniqueGPSPointsMap.value(),
                                null, //broadcastUserFileNameTotalResSecondsOfAllVisitsMap.value(),
                                // broadcastUserFileNameLocationWeightedCumulativeGaussianMap.value(),
                                broadcastEpsilon.value(),
                                broadcastMinPoints.value(),
                                broadcastDistanceMeasure.value());

//                            Utils.distributedExtractGPSAvgSignLocAvgSignTime(broadcastDataset.value(),
//                                    broadcastPPUserFilePaths.value(),
//                                    thresholdT,
//                                    userFileNameLocationWeightedCumulativeGaussianMap,
//                                    broadcastEpsilon.value(),
//                                    broadcastMinPoints.value(),
//                                    broadcastDistanceMeasure.value());
                    } // call
                });


        Map<Double, Triple<Double, Double, Double>> resultsMap = new TreeMap<>(resultsRDD.collectAsMap());
        thresholdsRDD.unpersist();
        thresholdsRDD = null;
        resultsRDD.unpersist();
        resultsRDD = null;
        broadcastDataset.unpersist();
        broadcastPPUserFilePaths.unpersist();
        //broadcastUserFileNameLocationWeightedCumulativeGaussianMap.unpersist();

        //broadcastUserFileNameMergedVisitsMap.unpersist();
        //broadcastUserFileNameUniqueGPSPointsMap.unpersist();
        //broadcastUserFileNameTotalResSecondsOfAllVisitsMap.unpersist();


        return resultsMap;
    } // distributedGPSAvgSignLocAvgSignTime


    //method to to run NextPlace algorithm on spark in a distributed fashion
    //1 MAP step is performed => file path -> global result string
    public static List<Tuple2<String, String>> doDistributedNextPlace(Dataset dataset, JavaSparkContext sc,
                                                                      List<String> ppHDFSUserFilePaths, int requiredPartitionCount,
                                                                      NextPlaceConfiguration npConf, boolean calculatePE,
                                                                      double trainSplitFraction, double testSplitFraction,
                                                                      long timeTInDaySeconds,
                                                                      long deltaTSeconds,
                                                                      long errorMarginInSeconds) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();

        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Running Distributed NextPlace algorithm on " + dataset + " .....");
        } // if


        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
        final Broadcast<Boolean> broadcastCalculatePE = sc.broadcast(calculatePE);
        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
        final Broadcast<Long> broadcastTimeTInDaySeconds = sc.broadcast(timeTInDaySeconds);
        final Broadcast<Long> broadcastDeltaTSeconds = sc.broadcast(deltaTSeconds);
        final Broadcast<Long> broadcastErrorMarginInSeconds = sc.broadcast(errorMarginInSeconds);

        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppHDFSUserFilePaths, requiredPartitionCount);
        //System.out.println("Pre-processed file paths rDD data count: " + ppUserFilePathsRDD.count());


        //print configured file system
        System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism: " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count: " + requiredPartitionCount);
        System.out.println("Partitions count of the created RDD: " + ppUserFilePathsRDD.partitions().size() + "\n");


        //you should get a result back, otherwise "lazy evaluation paradigm" of Spark won't do anything
        //mapToPair() will map to (String -> preProcessedUserFilePath, String -> globalResultString)
        JavaPairRDD<String, String> ppUserFilePathResultStringPairRDD = ppUserFilePathsRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    private static final long serialVersionUID = -7221781932454717127L;

                    @Override
                    public Tuple2<String, String> call(String ppUserFilePath) throws Exception {
                        //now the get input from the given file path regardless of sparks' local mode or cluster mode
                        InputStream inputStream = inputStreamFromHDFSFilePath(broadcastConf.value().value(),
                                ppUserFilePath);

                        String globalResultString = Utils.doNextPlace(ppUserFilePath, inputStream,
                                broadcastNpConf.value(),
                                broadcastCalculatePE.value(),
                                broadcastTrainSplitFraction.value(),
                                broadcastTestSplitFraction.value(),
                                broadcastTimeTInDaySeconds.value(),
                                broadcastDeltaTSeconds.value(),
                                broadcastErrorMarginInSeconds.value());

                        //aggregate preProcessedUserFilePath and globalResultString pair in JavaPairRDD
                        return new Tuple2<String, String>(ppUserFilePath, globalResultString);
                    } // call
                });

        //collect pp user file path and global result string pair of each user,
        //collect() here will avoid "lazy evaluation paradigm"
        List<Tuple2<String, String>> list = ppUserFilePathResultStringPairRDD.collect();

        //unpersist previous rdds and broadcast configuration, and make them ready for GC
        ppUserFilePathsRDD.unpersist();
        ppUserFilePathResultStringPairRDD.unpersist();
        broadcastConf.unpersist();
        //resultRDD.unpersist(); resultRDD = null;
        broadcastTrainSplitFraction.unpersist();
        broadcastTestSplitFraction.unpersist();
        ppUserFilePathsRDD = null;
        ppUserFilePathResultStringPairRDD = null;


        long endTime = System.nanoTime();
        System.out.println("||-----------------------------------------------------------------------------------||");
        System.out.println("|| Distributed NextPlace run took: " + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");

        //return result pairs
        return list;
    } // doDistributedNextPlace


    //method to to run NextPlace algorithm on spark in a distributed fashion
    //2 MAP steps is performed => file path -> file content -> global result string
    public static Map<String, String> doDistributedNextPlace(Dataset dataset,
                                                             JavaSparkContext sc, int requiredPartitionCount,
                                                             ArrayList<String> ppHDFSUserFilePaths,
                                                             NextPlaceConfiguration npConf,
                                                             boolean calculatePE,
                                                             double trainSplitFraction,
                                                             double testSplitFraction,
                                                             long timeTInDaySeconds,
                                                             long deltaTSeconds,
                                                             long errorMarginInSeconds) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();

        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Running Distributed NextPlace algorithm on " + dataset + " .....");
        } // if


        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
        final Broadcast<Boolean> broadcastCalculatePE = sc.broadcast(calculatePE);
        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
        final Broadcast<Long> broadcastTimeTInDaySeconds = sc.broadcast(timeTInDaySeconds);
        final Broadcast<Long> broadcastDeltaTSeconds = sc.broadcast(deltaTSeconds);
        final Broadcast<Long> broadcastErrorMarginInSeconds = sc.broadcast(errorMarginInSeconds);

        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppHDFSUserFilePaths, requiredPartitionCount);
        //System.out.println("Pre-processed file paths rDD data count: " + ppUserFilePathsRDD.count());

        //print configured file system
        System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism = " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count = " + requiredPartitionCount);
        System.out.println("Partitions count of the created RDD = " + ppUserFilePathsRDD.partitions().size() + "\n");


        //you should get a result back, otherwise "lazy evaluation paradigm" of Spark won't do anything
        //generate pp user file path and file contents pair rdd
        JavaPairRDD<String, String> ppUserFilePathAndFileContentsPairRDD
                = ppUserFilePathsRDD.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = -4161693627273311013L;

            @Override
            public Tuple2<String, String> call(String ppUserFilePath) throws Exception {
                //get file contents from hdfs
                String fileContents = Utils.fileContentsFromHDFSFilePath(broadcastConf.value().value(),
                        ppUserFilePath, false);

                //return ppUserFilePath and fileContents tuple
                return new Tuple2<String, String>(ppUserFilePath, fileContents);
            } // call
        });


        //ONE MORE STEP: to run doNextPlace algorithm on file contents
        //you should get a result back, otherwise "lazy evaluation paradigm" of Spark won't do anything
        //mapToPair() will map to (String -> preProcessedUserFilePath, String -> globalResultString)
        JavaPairRDD<String, String> ppUserFilePathResultStringPairRDD
                = ppUserFilePathAndFileContentsPairRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            private static final long serialVersionUID = -882746411482393602L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                //get ppUserFilePath from tuple
                String ppHDFSUserFilePath = stringStringTuple2._1;
                String fileContents = stringStringTuple2._2;

                //run the algorithm on file contents and global string back
                String globalResultString = Utils.doNextPlace(ppHDFSUserFilePath, fileContents,
                        broadcastNpConf.value(),
                        broadcastCalculatePE.value(),
                        broadcastTrainSplitFraction.value(),
                        broadcastTestSplitFraction.value(),
                        broadcastTimeTInDaySeconds.value(),
                        broadcastDeltaTSeconds.value(),
                        broadcastErrorMarginInSeconds.value());

                //return the tuple2 od ppUserFilePath and globalResultString
                return new Tuple2<String, String>(ppHDFSUserFilePath, globalResultString);
            } // call
        });

        //collect pp user file path and global result string pair of each user,
        //collect() here will avoid "lazy evaluation paradigm"
        Map<String, String> map = ppUserFilePathResultStringPairRDD.collectAsMap();


        //unpersist previous rdds and broadcast conf, and make them ready for GC
        ppUserFilePathsRDD.unpersist();
        ppUserFilePathAndFileContentsPairRDD.unpersist();
        ppUserFilePathResultStringPairRDD.unpersist();
        broadcastConf.unpersist();
        broadcastNpConf.unpersist();
        broadcastCalculatePE.unpersist();
        broadcastTrainSplitFraction.unpersist();
        broadcastTestSplitFraction.unpersist();
        ppUserFilePathsRDD = null;
        ppUserFilePathAndFileContentsPairRDD = null;
        ppUserFilePathResultStringPairRDD = null;


        long endTime = System.nanoTime();
        System.out.println("||-----------------------------------------------------------------------------------||");
        System.out.println("|| Distributed NextPlace run took: " + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");

        //return result pairs
        return map;
    } // doDistributedNextPlace


    //helper method to calculate distributed NextPlace on the files in HDFS directory
    public static List<Tuple2<String, String>> doDistributedNextPlace(Dataset dataset,
                                                                      JavaSparkContext sc,
                                                                      int requiredPartitionCount,
                                                                      String hdfsDirPathOfPPUserFiles,
                                                                      boolean recursive,
                                                                      NextPlaceConfiguration npConf,
                                                                      boolean calculatePE,
                                                                      double trainSplitFraction,
                                                                      double testSplitFraction,
                                                                      long timeTInDaySeconds,
                                                                      long deltaTSeconds,
                                                                      long errorMarginInSeconds) {
        //list file from hdfs dir
        List<String> ppUserFilePaths
                = Utils.listFilesFromHDFSPath(sc.hadoopConfiguration(), hdfsDirPathOfPPUserFiles, recursive);

        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting list of files is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //return an empty list tuple2
            return new ArrayList<Tuple2<String, String>>();
        } else    // paths contain at least one file
        {
            //return the result of Distributed NextPlace run on file paths
            return doDistributedNextPlace(dataset, sc, ppUserFilePaths, requiredPartitionCount,
                    npConf, calculatePE, trainSplitFraction, testSplitFraction,
                    timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds);
        } // else
    } // doDistributedNextPlace


    //helper method to calculate distributed NextPlace on the files in HDFS directory
    public static void doDistributedNextPlaceAndWriteResults(Dataset dataset,
                                                             JavaSparkContext sc,
                                                             String hdfsDirPathOfPPUserFiles,
                                                             boolean recursive,
                                                             NextPlaceConfiguration npConf,
                                                             boolean calculatePEForEachFile,
                                                             double trainSplitFraction,
                                                             double testSplitFraction,
                                                             long timeTInDaySeconds,
                                                             long deltaTSeconds,
                                                             long errorMarginInSeconds,
                                                             String resultFileNameAppendix) {
        //list file from hdfs dir
        List<String> ppUserFilePaths
                = Utils.listFilesFromHDFSPath(sc.hadoopConfiguration(), hdfsDirPathOfPPUserFiles, recursive);

        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting list of files is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //since the file list is empty return from a method
            return;
        } else    // paths contain at least one file
        {
            //for finding distributed NextPlace run time
            long startTime = System.nanoTime();

            //print dataset info and s.o.p algorithm run message if not null
            if (dataset != null) {
                dataset.printInfo();
                System.out.println("Running Distributed NextPlace algorithm on " + dataset + " ......");
            } // if

            //report user that we also write algorithm results to files
            System.out.println("Writing NextPlace results to files in a distributed mode ......");


            //will be used to get hadoop configuration inside transformation functions;
            //since SparkContext is NOT usable inside transformation functions
            final Broadcast<SerializableWritable<Configuration>> broadcastHadoopConf
                    = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

            final Broadcast<Boolean> broadcastCalculatePEForEachFile = sc.broadcast(calculatePEForEachFile);
            final Broadcast<String> broadcastResultFileNameAppendix = sc.broadcast(resultFileNameAppendix);
            final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
            final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
            final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
            final Broadcast<Long> broadcastTimeTInDaySeconds = sc.broadcast(timeTInDaySeconds);
            final Broadcast<Long> broadcastDeltaTSeconds = sc.broadcast(deltaTSeconds);
            final Broadcast<Long> broadcastErrorMarginInSeconds = sc.broadcast(errorMarginInSeconds);


            //numSlices to partition the user files
            int numFiles = ppUserFilePaths.size();
            int numSlices = numFiles > 300000 ? numFiles / 1000 : (numFiles > 100000 ? numFiles / 100 :
                    (numFiles > 10000 ? numFiles / 10 : numFiles));


            //get rdd of preprocessed user file paths
            JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, numSlices);
            //System.out.println("Pre-processed file paths rDD data count: " + ppUserFilePathsRDD.count());


            //print configured file system
            System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
            //prints the number of processors/workers or threads that spark will execute
            System.out.println("Default parallelism: " + sc.defaultParallelism());
            //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
            System.out.println("Required partition count: " + numSlices);
            System.out.println("Partitions count of the created RDD: " + ppUserFilePathsRDD.partitions().size() + "\n");


            //you should get a result back, otherwise "lazy evaluation paradigm" of Spark won't do anything
            //mapToPair() will map to (String -> preProcessedUserFilePath, String -> globalResultString)
            JavaPairRDD<String, String> ppUserFilePathResultStringPairRDD = ppUserFilePathsRDD.mapToPair(
                    new PairFunction<String, String, String>() {
                        private static final long serialVersionUID = -7221781932454717127L;

                        @Override
                        public Tuple2<String, String> call(String ppUserFilePath) throws Exception {
                            //now the get input from the given file path regardless of sparks' local mode or cluster mode
                            InputStream inputStream = inputStreamFromHDFSFilePath(broadcastHadoopConf.value().value(),
                                    ppUserFilePath);

                            String globalResultString = Utils.doNextPlace(ppUserFilePath, inputStream,
                                    broadcastNpConf.value(),
                                    broadcastCalculatePEForEachFile.value(),
                                    broadcastTrainSplitFraction.value(),
                                    broadcastTestSplitFraction.value(),
                                    broadcastTimeTInDaySeconds.value(),
                                    broadcastDeltaTSeconds.value(),
                                    broadcastErrorMarginInSeconds.value());

                            //aggregate preProcessedUserFilePath and globalResultString pair in JavaPairRDD
                            return new Tuple2<String, String>(ppUserFilePath, globalResultString);
                        } // call
                    });


            //Void resultRDD to write the results back to HDFS
            JavaRDD<Void> resultRDD = ppUserFilePathResultStringPairRDD.map(
                    new Function<Tuple2<String, String>, Void>() {
                        private static final long serialVersionUID = 2102747403689680776L;

                        @Override
                        public Void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                            String ppUserFilePath = stringStringTuple2._1;
                            String globalResultString = stringStringTuple2._2;

                            String fileName = fileNameFromPath(ppUserFilePath);
                            //String currentFileExtension = "." + FilenameUtils.getExtension(fileName);
                            String newFileExtension = ".txt";

                            //write to hdfs
                            Utils.writeToHDFS(broadcastHadoopConf.value().value(), ppUserFilePath,
                                    broadcastResultFileNameAppendix.value(),
                                    "",
                                    //currentFileExtension,
                                    newFileExtension,
                                    globalResultString);

                            return null;
                        } // call
                    });

            //collect Void rdd to avoid spark's "lazy evaluation paradigm"
            resultRDD.collect();

            //unpersist previous rdds and broadcast configuration, and make them ready for GC
            ppUserFilePathsRDD.unpersist();
            ppUserFilePathResultStringPairRDD.unpersist();
            resultRDD.unpersist();
            broadcastHadoopConf.unpersist();
            broadcastTrainSplitFraction.unpersist();
            broadcastTestSplitFraction.unpersist();
            broadcastResultFileNameAppendix.unpersist();
            broadcastCalculatePEForEachFile.unpersist();
            ppUserFilePathsRDD = null;
            ppUserFilePathResultStringPairRDD = null;
            resultRDD = null;


            //report distributed algorithm run time to standard output
            long endTime = System.nanoTime();
            System.out.println("\n||-----------------------------------------------------------------------------------||");
            System.out.println("|| Distributed NextPlace run and writing of NextPlace results took: " + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");

            //print message to indicate where the result files are
            System.out.println("|| Check " + "\"" + resultFileNameAppendix + "\"" + " subdirectory of "
                    + "\"" + hdfsDirPathOfPPUserFiles + "\"" + " directory for the results of each preprocessed user file");
        } // else

    } // doDistributedNextPlaceAndWriteResults


    /**
     * Method to perform NextPlace algorithm on pre-processed user file input stream
     *
     * @param ppUserFilePath        pre-processed file path of user considered
     * @param ppUserFileInputStream input stream from a user file which can either local or network file
     * @param calculatePE           should calculate predictability error for this NextPlace run
     * @param npConf                NextPlaceConfiguration instance that contains m, v, timeT and deltaT
     * @return results of NextPlace calculations
     */
    public static String doNextPlace(String ppUserFilePath, InputStream ppUserFileInputStream,
                                     NextPlaceConfiguration npConf, boolean calculatePE,
                                     double trainSplitFraction,
                                     double testSplitFraction,
                                     long timeTInDaySeconds,
                                     long deltaTSeconds,
                                     long errorMarginThetaInSeconds) {
        //global result string builder that will build result string which will contain
        //the result of NextPlace algorithm for each preprocessed user file
        StringBuilder globalResultStringBuilder = new StringBuilder("");

        //name of the file considered
        String ppUserFileName = fileNameFromPath(ppUserFilePath);

        //get merged visits from ppUserFile
        ArrayList<Visit> mergedVisits = visitsFromPPUserFileInputStream(ppUserFileInputStream);

        //and return the results of calculation from helper method
        return resultsFromNextPlaceRun(globalResultStringBuilder, ppUserFileName, mergedVisits,
                npConf, calculatePE, trainSplitFraction, testSplitFraction, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);


        //sensor readings starts from the end of the file; sorted with Date comparison

        //for having ( u, l, {(t_0, d_0),(t_1, d_1), ..., (t_n, d_n)} ) structure:
        //mapped file name to "user"
        //mapped (longitude, latitude) to "location"
        //merge operation is perform on unix timestamps
        //after merge "arrival time"
        //and "residence time" is generated
        //at the end, time series for each visit history
        //are created in the form {(t_0, d_0),(t_1, d_1), ..., (t_n, d_n)}
    } // doNextPlace


    //doNextPlace() algorithm that accepts the directory of pp-ed files and runs the NextPlace algorithm on each of them;
    //return the list of global result string to write to the file system
    //if the given path is actually a file, it will return the result for that file
    public static List<String> doNextPlace(Dataset dataset, String localDirPathOfPPUserFiles, boolean recursive,
                                           NextPlaceConfiguration npConf,
                                           boolean calculatePEForEachFile,
                                           double trainSplitFraction, double testSplitFraction,
                                           long timeTInDaySeconds,
                                           long deltaTSeconds,
                                           long errorMarginInSeconds) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();


        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Running Distributed NextPlace algorithm on " + dataset + " .....");
        } // if


        //list of global result strings
        List<String> globalResultStrings = new ArrayList<String>();

        //get the files from given path; if the given path is actually a file, only the result for that file will be returned
        List<String> ppUserFilePaths = listFilesFromLocalPath(localDirPathOfPPUserFiles, recursive);


        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting list of files is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //return an empty list that will not contain any result string
            return new ArrayList<String>();
        } // if
        else {
            //now run NextPlace algorithm for each file path
            for (String ppUserFilePath : ppUserFilePaths) {
                System.out.println("\nRunning NextPlace algorithm on file in path: " + ppUserFilePath + " .....");

                //get the file contents for the file in path, P.S. running np with input stream of the file is also possible
                String fileContents = fileContentsFromLocalFilePath(ppUserFilePath);

                //run the NextPlace
                String globalResultString = doNextPlace(ppUserFilePath, fileContents, npConf,
                        calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                        timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds);

                //add the result string to the list
                globalResultStrings.add(globalResultString);
            } // for
        } // else


        long endTime = System.nanoTime();
        System.out.println("||-----------------------------------------------------------------------------------||");
        System.out.println("|| Single-machine NextPlace run took: " + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");

        //return the list
        return globalResultStrings;
    } // doNextPlace


    //helper method to run NextPlace algorithm in a single machine mode and write the results to sub-directory
    //named <resultFileNameAppendix>;
    //writing operation to the result file will be sequential
    public static void doNextPlaceAndWriteResults(Dataset dataset, String localDirPathOfPPUserFiles, boolean recursive,
                                                  NextPlaceConfiguration npConf, boolean calculatePEForEachFile,
                                                  double trainSplitFraction, double testSplitFraction,
                                                  long timeTInDaySeconds, long deltaTSeconds, long errorMarginInSeconds,
                                                  String resultFileNameAppendix) {
        //for finding distributed NextPlace run and sequential writing run time
        long startTime = System.nanoTime();

        //generate pp user file paths
        List<String> ppUserFilePaths = listFilesFromLocalPath(localDirPathOfPPUserFiles, recursive);

        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting list of files is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //return from a method since list of pp user file paths is empty
            return;
        } // if
        else {
            //now run NextPlace algorithm for each file path
            for (String ppUserFilePath : ppUserFilePaths) {
                System.out.println("\nRunning NextPlace algorithm on file in path: " + ppUserFilePath + " .....");

                //get the file contents for the file in path, P.S. running np with input stream of the file is also possible
                String fileContents = fileContentsFromLocalFilePath(ppUserFilePath);

                //run the NextPlace and get the result
                String globalResultString = doNextPlace(ppUserFilePath, fileContents, npConf,
                        calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                        timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds);

                System.out.println("Writing NextPlace results for " + ppUserFilePath + " .....");


                String fileName = fileNameFromPath(ppUserFilePath);
                String currentFileExtension = "." + FilenameUtils.getExtension(fileName);


                //write result locally to the file whose name is built with relevant appendix from ppUserFilePath
                //provide "" file id to have single resultFileNameAppendix file for a user
                writeToLocalFileSystem(ppUserFilePath, resultFileNameAppendix, "", currentFileExtension, globalResultString);
            } // for


            //print message to indicate where the result files are
            System.out.println("\nCheck " + resultFileNameAppendix + " subdirectory of "
                    + localDirPathOfPPUserFiles + " directory for results of each preprocessed user file");


            long endTime = System.nanoTime();
            System.out.println("\n||-----------------------------------------------------------------------------------||");
            System.out.println("|| Single-machine NextPlace run and sequential writing of results took: "
                    + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");
        } // else
    } // doNextPlaceAndWriteResults


    //overloaded version of doNextPlace which does NOT do pre-processed file //write and// read operation;
    //will run next place algorithm on already read file contents
    public static String doNextPlace(String ppUserFilePath, String ppUserFileContents,
                                     NextPlaceConfiguration npConf, boolean calculatePE,
                                     double trainSplitFraction, double testSplitFraction,
                                     long timeTInDaySeconds, long deltaTSeconds,
                                     long errorMarginThetaInSeconds) {
        //global result string builder that will build result string which will contain
        //the result of NextPlace algorithm for each preprocessed user file
        StringBuilder globalResultStringBuilder = new StringBuilder("");

        //name of the file considered
        String ppUserFileName = fileNameFromPath(ppUserFilePath);

        //obtain significant visits (visits performed to significant locations) from ppUserFileContents;
        //distributed next place algorithm works on visits performed to significant locations
        ArrayList<Visit> significantVisits = visitsFromPPUserFileContents(ppUserFileContents);

        //and return the results of calculation from helper method
        return resultsFromNextPlaceRun(globalResultStringBuilder, ppUserFileName, significantVisits, npConf,
                calculatePE, trainSplitFraction, testSplitFraction, timeTInDaySeconds, deltaTSeconds,
                errorMarginThetaInSeconds);
    } // doNextPlace


    //overloaded method to get the results from NextPlace algorithm run;
    //it is more efficient in terms of runtime
    public static String resultsFromNextPlaceRun(StringBuilder globalResultStringBuilder,
                                                 String fileName, ArrayList<Visit> significantVisits,
                                                 NextPlaceConfiguration npConf,
                                                 boolean calculatePE,
                                                 double trainSplitFraction,
                                                 double testSplitFraction,
                                                 long timeTInDaySeconds,
                                                 long deltaTSeconds,
                                                 long errorMarginThetaInSeconds) {
        //populate global result string with info
        globalResultStringBuilder.append("======= File "
                + "\"" + fileName + "\""
                + ": NextPlace prediction results =======\n");

        //above method will return empty array list if file reading or any other problem occurs;
        //it will also return empty list if pre-processed file is empty;
        //if both cases happen, return from method
        if (significantVisits.isEmpty()) return globalResultStringBuilder.toString();

        int highestFrequency = highestVisitFrequencyOfUser(significantVisits);
        //maxK related to the highest visit frequency cannot be equal to or smaller than 0
        //otherwise user file is not predictable, because future visits
        //cannot be predicted from file's highest frequent visit (location, which will correspond to visit history)
        int maxKOfHighestFrequentVisitHistory = maxValueOfFutureStepK(npConf, highestFrequency);
        if (maxKOfHighestFrequentVisitHistory <= 0) {
            //skip this user, by continuing the loop
            //System.out.println("\n======= File "
            //        + "\"" + fileName + "\""
            //        + " is skipped since the user it belongs to has highest visit frequency = "
            //        + highestFrequency
            //        + " whose maxK = "
            //        + maxKOfHighestFrequentVisitHistoryOfUser + " which cannot be 0 or negative"
            //        + " =======\n");


            globalResultStringBuilder.append("\n======= File "
                    + "\"" + fileName + "\""
                    + " is skipped since the user it belongs to has highest visit frequency = "
                    + highestFrequency
                    + " whose maxK = "
                    + maxKOfHighestFrequentVisitHistory + " which cannot be 0 or negative"
                    + " =======\n");

            //do not perform further calculations; return from the method
            return globalResultStringBuilder.toString();
        } // if


        //Here maxKOfHighestFrequentVisitHistoryOfUser is at least 1, so
        //create array list of visit histories to different unique locations;
        //this createVisitHistories() method is more reliable, since
        //visit history of visits whose maxK <= 0 will not be created;
        //create visit histories based on train - test split possibility
        Collection<VisitHistory> visitHistories;
        if (((int) (trainSplitFraction * 100)) == 100)     // if train split is 100%, do not do divisibility check
            visitHistories = createVisitHistories(significantVisits, npConf, false);
        else
            visitHistories = createVisitHistories(significantVisits, npConf, trainSplitFraction, testSplitFraction);


        //get the training portion depending on a train and test split fraction;
        //if training portion is 100% do nothing;
        //correctness of train and test split fractions are checked in createVisitHistories method
        if (((int) (trainSplitFraction * 100)) < 100) {
            ArrayList<VisitHistory> firstPartVisitHistories = new ArrayList<>();
            for (VisitHistory visitHistory : visitHistories) {
                VisitHistory firstPart = visitHistory.getTrainSplit(npConf, trainSplitFraction, testSplitFraction);
                firstPartVisitHistories.add(firstPart);
            } // for

            //assign first parrt visit histories to visit histories
            //where only first part visit histories will be used in prediction
            visitHistories = firstPartVisitHistories;
        } // else


        //write the visit history to the file
        //System.out.println("======= For the user with file name"
        //        + "\"" + fileName + "\"; visit histories"
        //        + " with predictability rate (maxK) of at least "
        //        + 1 + " are chosen =======");


        globalResultStringBuilder.append("======= For the user with file name "
                + "\"" + fileName + "\"; visit histories"
                + " with predictability rate (maxK) of at least "
                + 1 + " are chosen =======\n");


        //print all visit histories
        int count = 0;
        for (VisitHistory visitHistory : visitHistories) {
            //write each visitHistory with writer
            //System.out.println("======= Visit history #" + (count + 1)
            //        + "; Frequency: " + visitHistory.getFrequency()
            //        + "; MaxK: " + visitHistory.maxPossibleK(npConf)
            //        + " =======");
            //userNextPlaceResultsWriter.println(visitHistory.toStringWithDate());


            globalResultStringBuilder.append("======= Visit history #" + (count + 1)
                    + "; Frequency: " + visitHistory.getFrequency()
                    + "; MaxK: " + visitHistory.maxPossibleK(npConf)
                    + " =======\n");
            //globalResultStringBuilder.append(visitHistory.toStringWithDate() + "\n");

            //increment count
            count++;
        } // for

        //lowestVisitFrequencyOfUser() will return lowest visit frequency of all locations
        //which user has visited
        //int lowestVisitHistoryTimeSeriesSize = lowestTimeSeriesSizeOfAllCreatedVisitHistories(visitHistories);


        //maximum value of K of lowest frequent visit history for predicting next k visits
        //int maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories = maxValueOfFutureStepK(npConf, lowestVisitHistoryTimeSeriesSize);

        //write some necessary initialization info to print writer
        //System.out.println("======= Maximum value of K = "
        //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======");
        //System.out.println("======= Highest visit frequency of all"
        //        + " predictable" // NEW reliable method
        //        + " visit histories of a user = "
        //        + highestFrequency + " =======");

        //lowest visit frequency here, is always bigger or equal to "frequencyThreshold" defined above
        //because we have created visit histories which have at least "frequencyThreshold" frequency
        //System.out.println("======= Lowest visit frequency of all"
        //        + " predictable" // NEW reliable method
        //        + " visit histories of a user = "
        //        + lowestVisitHistoryTimeSeriesSize + " =======");
        //System.out.println("======= Embedding parameter M = "
        //        + embeddingParameterM + " =======");
        //System.out.println("======= Delay parameter V = "
        //        + delayParameterV + " =======");
        //System.out.println("======= timeT in day seconds = "
        //        + timeTInDaySeconds + " =======");
        //System.out.println("======= deltaT in seconds = "
        //        + deltaTSeconds + " =======");


        //globalResultStringBuilder.append("======= Maximum value of K = "
        //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======\n");

        globalResultStringBuilder.append("======= Highest visit frequency of all"
                + " predictable" // NEW reliable method
                + " visit histories of a user = "
                + highestFrequency + " =======\n");
        globalResultStringBuilder.append("======= MaxK of highest frequent visit history = "
                + maxKOfHighestFrequentVisitHistory + " =======\n");

        //lowest visit frequency here, is always bigger or equal to "frequencyThreshold" defined above
        //because we have created visit histories which have at least "frequencyThreshold" frequency
        //globalResultStringBuilder.append("======= Lowest visit frequency of all"
        //        + " predictable" // NEW reliable method
        //        + " visit histories of a user = "
        //        + lowestVisitHistoryTimeSeriesSize + " =======\n");
        //globalResultStringBuilder.append("======= MaxK of lowest frequent visit history = "
        //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======\n");
        globalResultStringBuilder.append("======= Embedding parameter M = "
                + npConf.getEmbeddingParameterM() + " =======\n");
        globalResultStringBuilder.append("======= Delay parameter V = "
                + npConf.getDelayParameterV() + " =======\n");
        globalResultStringBuilder.append("======= timeT in day seconds = "
                + timeTInDaySeconds + " =======\n");
        globalResultStringBuilder.append("======= deltaT in seconds = "
                + deltaTSeconds + " =======\n");


        //can be safely disabled later
        //predictability error of a user;
        //peOfPPUserFile will create its own visit histories
        //considering every single visit from significantVisits
        if (calculatePE) // if user is asked to calculate predictability error
        {
            double peOfAUser
                    = peOfPPUserFile(npConf, significantVisits, trainSplitFraction, testSplitFraction);

            //System.out.println("======= Predictability error of all visits of a user with file name "
            //        + "\"" + fileName + "\"" + ": "
            //        + peOfAUser + " =======");


            globalResultStringBuilder.append("======= Predictability error of all" +
                    " visits of a user with file name " + "\"" + fileName + "\"" + ": "
                    + peOfAUser + " =======\n");
        } // if
        else {
            //write not calculated with println
            //System.out.println("======= Predictability error of all" +
            //        " visits of a user with file name " + "\"" + fileName + "\"" + ": "
            //        + " not calculated " + " =======");

            globalResultStringBuilder.append("======= Predictability error of all" +
                    " visits of a user with file name " + "\"" + fileName + "\"" + ": "
                    + " not calculated " + " =======\n");
        } // else


        //create an instance of NextPlace with embedding parameter m and delay parameter v
        NextPlace algorithm = new NextPlace(npConf);


        //create three has maps for visit histories
        //first, visit history => embedding space alphaGlobal map
        //second, visit history => distance-index-pairs map
        //third, visit history => standard deviation sigma map
        //HashMap<Integer, Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>> vhAlphaGlobalMap = new HashMap<>();
        //HashMap<Integer, SetMultimap<Double, Integer>> vhDistIndexPairsMap = new HashMap<>();
        //HashMap<Integer, Double> vhStdMap = new HashMap<>();
        //HashMap<Integer, Integer> vhNumNeighborsMap = new HashMap<>();
        HashMap<Integer, Integer> vhIndexOfBetaNMap = new HashMap<>();
        HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap = new HashMap<>();
        HashMap<Integer, Integer> vhLimitFutureStepKMap = new HashMap<>();

        //by default
        int maxLimitFutureStepK = 16;    //can also defined as 8, to keep good statistical potential of visit histories

        //now for each visit history calculate distance index pairs, alpha global and std
        for (VisitHistory visitHistory : visitHistories)
        {
            //location of this visit history
            Integer hashOfVh = visitHistory.hashCode();

            //time series of this visit history
            ArrayList<TimeSeriesInstant> timeSeriesOfThisVisitHistory = visitHistory.getTimeSeries();

            //create an embedding space for the time series of this visit history
            Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
                    = algorithm.createEmbeddingSpace(timeSeriesOfThisVisitHistory);
            LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
            int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
            EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);

            //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
            //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
            //where Double.compareTo() method uses long bits of a Double number;
            //we will sort values of this map where indices are in descending order to collect recent indexed neighbors first
            //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]};


            //FOR EPSILON NEIGHBORHOOD WE DO NOT NEED SET MULTI MAP, LINKED HASH MULTI MAP WOULD BE ENOUGH
            //SetMultimap<Double, Integer> distanceIndexPairs
            //        = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
            //= LinkedHashMultimap.create();
            //iterate over all B_n in alphaGlobal,
            //alphaGlobal contains keys sorted in ascending order
            //alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
            //for (Integer nIndex : alphaGlobal.keySet()) {
            //    double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
            //    distanceIndexPairs.put(distance, nIndex);
            //} // for
            //alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place


            //calculate std
            double stdOfTimeSeriesOfVisitHistory = Math.sqrt(Utils.populationVarianceOfTimeSeries(timeSeriesOfThisVisitHistory));
            LinkedHashMap<Integer, EmbeddingVector> neighborhood
                    = algorithm.epsN(stdOfTimeSeriesOfVisitHistory, alphaGlobal, indexOfBeta_N, beta_N);

            //calculate the limit on future step k
            int limitFutureStepK = visitHistory.maxPossibleK(npConf);

            //obtain max limit future step K
            if (limitFutureStepK > maxLimitFutureStepK)
                maxLimitFutureStepK = limitFutureStepK;

            //numNeighbors can be calculated in such a way that it decreases PE
            //the method is bestPEBestNumNeighbors()
            //for each vh (and for each futureStepK) different number of neighbors should be calculated
            //max future step K will be maxKOfHighestFrequentVisitHistory
            //int numNeighbors = alphaGlobal.size() / 2;
            //if (numNeighbors == 0) numNeighbors = 1; // handle the possible effect of integer division


            //put the results in corresponding maps
            //vhAlphaGlobalMap.put(hashOfVh, indexOfBeta_N_alphaGlobalTuple2);
            //vhDistIndexPairsMap.put(hashOfVh, distanceIndexPairs);
            //vhStdMap.put(hashOfVh, stdOfTimeSeriesOfVisitHistory);
            //vhNumNeighborsMap.put(hashOfVh, numNeighbors);
            vhIndexOfBetaNMap.put(hashOfVh, indexOfBeta_N);
            vhNeighborhoodMap.put(hashOfVh, neighborhood);
            vhLimitFutureStepKMap.put(hashOfVh, limitFutureStepK);
        } // for


        //tree set data structure will ensure the chronological ordering of predicted visits
        TreeSet<Visit> sequenceOfPredictedVisits = new TreeSet<>();
        for (VisitHistory vh : visitHistories)
        {
            Integer hashOfVH = vh.hashCode();
            //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
            //        = vhAlphaGlobalMap.get(hashOfVH);
            //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
            int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVH); //indexOfBeta_N_alphaGlobalTuple2._1;
            //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVH);
            //Double stdOfVH = vhStdMap.get(hashOfVH);
            LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVH);

            //vhNumNeighborsMap should contain arrays as values
            //each array will determine numNeighbors for its vh
            //array will have a size of maxKOfHighestFrequentVisitHistory
            //for vhs whose maxK smaller that this size will have -1 as numNeighbors in higher array indices
            //Integer numNeighbors = vhNumNeighborsMap.get(hashOfVH);


            Visit predictedVisit = algorithm.predictAVisitKStepsAhead(1,
                    vh,
                    //stdOfVH,
                    //alphaGlobal,
                    neighborhood,
                    indexOfBeta_N);
                    //distanceIndexPairs); //, numNeighbors);
            sequenceOfPredictedVisits.add(predictedVisit);
        } // for


        globalResultStringBuilder.append("\n======= Sequence of all predicted visits" +
                " in chronological order for deltaL = " + 1
                + "; #Predicted Visits: " + sequenceOfPredictedVisits.size() + "  =======\n");
        for (Visit v : sequenceOfPredictedVisits)
            globalResultStringBuilder.append(v.toStringWithDate()).append("\n");
        //make one more space
        globalResultStringBuilder.append("\n");


        //limit predictability rate K, when m is taken as 3, visit histories
        //should have size of at least 19, where in WiFi datasets this is 20, GPS datasets this is 19;
        //limitOnFutureStepK can also be defined as maxKOfHighestFrequentVisitHistories


        // you can pass, maxKOfHighestFrequentVisitHistory variable defined above as limitOnFutureStepK
        // provide error margin from method param
        //get the predicted visit at timeT + deltaT
        //as stated by the algorithm k always starts from 1
        Visit predictedVisitAtTimeTPlusDeltaT
                = algorithm
                //.pickVisitAtTimeTPlusDeltaTUpToPredLimit
                .pickVisitAtTimeTPlusDeltaTOriginal
                (globalResultStringBuilder, 1,
                //maxLimitFutureStepK,
                sequenceOfPredictedVisits, visitHistories,
                vhIndexOfBetaNMap,
                vhNeighborhoodMap,
                vhLimitFutureStepKMap,
                timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);


        //if predicted visit is non-null, write the details string builder
        if (predictedVisitAtTimeTPlusDeltaT != null) {
            //System.out.println("\nThe user will make this visit: "
            //        + predictedVisitAtTimeTPlusDeltaT.toStringWithDate()
            //        + "\nat timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400)
            //        + "\nto the location: " + predictedVisitAtTimeTPlusDeltaT.getLocation());
            globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");
            globalResultStringBuilder.append("The user will make this visit: "
                    + predictedVisitAtTimeTPlusDeltaT.toStringWithDate()
                    + "\nat timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400)
                    + "\nto the location: " + predictedVisitAtTimeTPlusDeltaT.getLocation() + "\n");
            globalResultStringBuilder.append("-----------------------------------------------------------------------\n");
        } else {
            //System.out.println("No location is predicted around day seconds"
            //        + " timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400));

            globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");
            globalResultStringBuilder.append("No location is predicted around day seconds"
                    + " timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400) + "\n");
            globalResultStringBuilder.append("-----------------------------------------------------------------------\n");
        } // else


        return globalResultStringBuilder.toString();
    } // resultsFromNextPlaceRun


//    //helper method to get the results back from NextPlace run
//    private static String resultsFromNextPlaceRun(StringBuilder globalResultStringBuilder,
//                                                  String fileName, ArrayList<Visit> mergedVisits,
//                                                  NextPlaceConfiguration npConf,
//                                                  boolean calculatePE,
//                                                  double trainSplitFraction, double testSplitFraction)
//    {
//        //System.out.println("======= File "
//        //        + "\"" + fileName + "\""
//        //        + ": NextPlace prediction results =======");
//
//        //populate global result string with info
//        globalResultStringBuilder.append("======= File "
//                + "\"" + fileName + "\""
//                + ": NextPlace prediction results =======\n");
//
//        //above method will return empty array list if file reading or any other problem occurs;
//        //it will also return empty list if pre-processed file is empty;
//        //if both cases happen, return from method
//        if (mergedVisits.isEmpty()) return globalResultStringBuilder.toString();
//
//
//
//        //--------------------- MORE RELIABLE METHOD ---------------------------------------
//        int highestFrequency = highestVisitFrequencyOfUser(mergedVisits);
//        //maxK related to the highest visit frequency cannot be equal to or smaller than 0
//        //otherwise user file is not predictable, because future visits
//        //cannot be predicted from file's highest frequent visit (location, which will correspond to visit history)
//        int maxKOfHighestFrequentVisitHistory = maxValueOfFutureStepK(npConf, highestFrequency);
//        if (maxKOfHighestFrequentVisitHistory <= 0)
//        {
//            //skip this user, by continuing the loop
//            //System.out.println("\n======= File "
//            //        + "\"" + fileName + "\""
//            //        + " is skipped since the user it belongs to has highest visit frequency = "
//            //        + highestFrequency
//            //        + " whose maxK = "
//            //        + maxKOfHighestFrequentVisitHistoryOfUser + " which cannot be 0 or negative"
//            //        + " =======\n");
//
//
//            globalResultStringBuilder.append("\n======= File "
//                    + "\"" + fileName + "\""
//                    + " is skipped since the user it belongs to has highest visit frequency = "
//                    + highestFrequency
//                    + " whose maxK = "
//                    + maxKOfHighestFrequentVisitHistory + " which cannot be 0 or negative"
//                    + " =======\n");
//
//            //do not perform further calculations; return from the method
//            return globalResultStringBuilder.toString();
//        } // if
//
//
//
//        //Here maxKOfHighestFrequentVisitHistoryOfUser is at least 1, so
//        //create array list of visit histories to different unique locations;
//        //this createVisitHistories() method is more reliable, since
//        //visit history of visits whose maxK <= 0 will not be created
//        Collection<VisitHistory> visitHistories = createVisitHistories(mergedVisits, npConf, false);
//
//        //write the visit history to the file
//        //System.out.println("======= For the user with file name"
//        //        + "\"" + fileName + "\"; visit histories"
//        //        + " with predictability rate (maxK) of at least "
//        //        + 1 + " are chosen =======");
//
//
//        globalResultStringBuilder.append("======= For the user with file name "
//                + "\"" + fileName + "\"; visit histories"
//                + " with predictability rate (maxK) of at least "
//                + 1 + " are chosen =======\n");
//        //---------------------  MORE RELIABLE METHOD ---------------------------------------
//
//
//
//        /*  // LESS RELIABLE METHOD
//        //------------------------------------------------------------------------------------------
//        //frequency threshold will be the half of the highest visit frequency
//        //so, visit histories will contain visits which are visited at least the half
//        //of the highest visit frequency;
//        //frequencyThreshold cannot be smaller than 2,
//        //otherwise we will not able to find neighborhood elements
//        int highestFrequency = highestVisitFrequencyOfUser(mergedVisits);
//        int frequencyThreshold;
//
//        //highest visit frequency cannot be smaller than 2
//        if (highestFrequency < 2)
//        {
//            //skip this user, by continuing the loop
//            //System.out.println("\n======= File "
//            //        + "\"" + fileName + "\""
//            //        + " is skipped since the user it belongs to has highest visit frequency = "
//            //        + highestFrequency + " which is smaller than 2 =======");
//
//
//            globalResultStringBuilder.append("\n======= File "
//                    + "\"" + fileName + "\""
//                    + " is skipped since the user it belongs to has highest visit frequency = "
//                    + highestFrequency + " which is smaller than 2 =======\n");
//
//            //do not perform further calculations; return from the method
//            return globalResultStringBuilder.toString();
//        } // if
//        //------------------------------------------------------------------------------------------
//
//
//        //------------------------------------------------------------------------------------------
//        //if it is 2 or 3, we take the visits highest frequency
//        //because integer division 3 / 2 = 1
//        else if (highestFrequency == 2 || highestFrequency == 3)
//            frequencyThreshold = highestFrequency;
//        else // otherwise with half of it
//            frequencyThreshold = highestFrequency / 2;
//
//
//        //create array list of visit histories to different unique locations
//        //HashSet is also possible, but changes visit history orders
//        //based on generated hash of the object
//        ArrayList<VisitHistory> visitHistories = createVisitHistories(mergedVisits,
//                frequencyThreshold);
//
//        //write the visit history to the file
//        //System.out.println("======= For the user with file name"
//        //        + "\"" + fileName + "\"; visit histories"
//        //        + " with frequency at least "
//        //        + frequencyThreshold + " are chosen =======");
//
//
//        globalResultStringBuilder.append("======= For the user with file name "
//                + "\"" + fileName + "\"; visit histories"
//                + " with frequency at least "
//                + frequencyThreshold + " are chosen =======\n");
//
//        */  // LESS RELIABLE METHOD
//
//
//
//        //print all visit histories
//        int count = 0;
//        for (VisitHistory visitHistory : visitHistories)
//        {
//            //write each visitHistory with writer
//            //System.out.println("======= Visit history #" + (count + 1)
//            //        + "; Frequency: " + visitHistory.getFrequency()
//            //        + "; MaxK: " + visitHistory.maxPossibleK(npConf)
//            //        + " =======");
//            //userNextPlaceResultsWriter.println(visitHistory.toStringWithDate());
//
//
//            globalResultStringBuilder.append("======= Visit history #" + (count + 1)
//                    + "; Frequency: " + visitHistory.getFrequency()
//                    + "; MaxK: " + visitHistory.maxPossibleK(npConf)
//                    + " =======\n");
//            globalResultStringBuilder.append(visitHistory.toStringWithDate() + "\n");
//
//            //increment count
//            count ++;
//        } // for
//        //------------------------------------------------------------------------------------------
//
//
//        //------------------------------------------------------------------------------------------
//        //lowestVisitFrequencyOfUser() will return lowest visit frequency of all locations
//        //which user has visited
//        int lowestVisitHistoryTimeSeriesSize = lowestTimeSeriesSizeOfAllCreatedVisitHistories(visitHistories);
//
//
//        //maximum value of K of lowest frequent visit history for predicting next k visits
//        int maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories = maxValueOfFutureStepK(npConf, lowestVisitHistoryTimeSeriesSize);
//
//        //write some necessary initialization info to print writer
//        //System.out.println("======= Maximum value of K = "
//        //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======");
//        //System.out.println("======= Highest visit frequency of all"
//        //        + " predictable" // NEW reliable method
//        //        + " visit histories of a user = "
//        //        + highestFrequency + " =======");
//
//        //lowest visit frequency here, is always bigger or equal to "frequencyThreshold" defined above
//        //because we have created visit histories which have at least "frequencyThreshold" frequency
//        //System.out.println("======= Lowest visit frequency of all"
//        //        + " predictable" // NEW reliable method
//        //        + " visit histories of a user = "
//        //        + lowestVisitHistoryTimeSeriesSize + " =======");
//        //System.out.println("======= Embedding parameter M = "
//        //        + embeddingParameterM + " =======");
//        //System.out.println("======= Delay parameter V = "
//        //        + delayParameterV + " =======");
//        //System.out.println("======= timeT in day seconds = "
//        //        + timeTInDaySeconds + " =======");
//        //System.out.println("======= deltaT in seconds = "
//        //        + deltaTSeconds + " =======");
//
//
//
//        //globalResultStringBuilder.append("======= Maximum value of K = "
//        //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======\n");
//
//        globalResultStringBuilder.append("======= Highest visit frequency of all"
//                + " predictable" // NEW reliable method
//                + " visit histories of a user = "
//                + highestFrequency + " =======\n");
//        globalResultStringBuilder.append("======= MaxK of highest frequent visit history = "
//                + maxKOfHighestFrequentVisitHistory + " =======\n");
//
//        //lowest visit frequency here, is always bigger or equal to "frequencyThreshold" defined above
//        //because we have created visit histories which have at least "frequencyThreshold" frequency
//        globalResultStringBuilder.append("======= Lowest visit frequency of all"
//                + " predictable" // NEW reliable method
//                + " visit histories of a user = "
//                + lowestVisitHistoryTimeSeriesSize + " =======\n");
//        globalResultStringBuilder.append("======= MaxK of lowest frequent visit history = "
//                + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories + " =======\n");
//        globalResultStringBuilder.append("======= Embedding parameter M = "
//                + npConf.getEmbeddingParameterM() + " =======\n");
//        globalResultStringBuilder.append("======= Delay parameter V = "
//                + npConf.getDelayParameterV() + " =======\n");
//        globalResultStringBuilder.append("======= timeT in day seconds = "
//                + npConf.getTimeTInDaySeconds() + " =======\n");
//        globalResultStringBuilder.append("======= deltaT in seconds = "
//                + npConf.getDeltaTSeconds() + " =======\n");
//        //------------------------------------------------------------------------------------------
//
//
//
//
//        //------------------------------------------------------------------------------------------
//        //predictability error of a user;
//        //peOfPPUserFile will create its own visit histories
//        //considering every single visit from mergedVisits,
//        //and in turn its own algorithm which takes the parameter
//        //of lowestVisitHistoryTimeSeriesSize of its own created visit histories;
//        if(calculatePE) // if user is asked to calculate predictability error
//        {
//            double peOfAllVisitsOfAUser
//                    = peOfPPUserFile(npConf, mergedVisits, trainSplitFraction, testSplitFraction);
//
//            //System.out.println("======= Predictability error of all visits of a user with file name "
//            //        + "\"" + fileName + "\"" + ": "
//            //        + peOfAllVisitsOfAUser + " =======");
//
//
//            globalResultStringBuilder.append("======= Predictability error of all" +
//                    " visits of a user with file name " + "\"" + fileName + "\"" + ": "
//                    + peOfAllVisitsOfAUser + " =======\n");
//        } // if
//        else
//        {
//            //write not calculated with println
//            //System.out.println("======= Predictability error of all" +
//            //        " visits of a user with file name " + "\"" + fileName + "\"" + ": "
//            //        + " not calculated " + " =======");
//
//            globalResultStringBuilder.append("======= Predictability error of all" +
//                    " visits of a user with file name " + "\"" + fileName + "\"" + ": "
//                    + " not calculated " + " =======\n");
//        } // else
//        //------------------------------------------------------------------------------------------
//
//
//
//        //NO NEED FOR CHECK SINCE ONLY PREDICTABLE VISIT HISTORIES ARE CREATED
//        /*  // LESS RELIABLE METHOD
//        //if maxK is smaller or equal to 0,
//        //then user's data set is not predictable according to the given configuration
//        //given maxK configuration is defined by lowestVisitHistoryTimeSeries size,
//        //embedding parameter m and delay parameter v
//        //WE WON'T PERFORM PREDICTIONS (chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations() method)
//        //since maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories is smaller than 0
//        if (maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories <= 0)
//        {
//            //System.out.println("\nFile "
//            //        + "\"" + fileName + "\""
//            //        + " is skipped since the user it belongs to has maximum possible K for predicting"
//            //        + "\nnext K visits = "
//            //        + "(lowestVisitHistoryTimeSeriesSize - 1) - (embeddingParameterM - 1) * delayParameterV = "
//            //        + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories
//            //        + "\nwhich cannot be 0 or negative"
//            //        + "\nplease try different combination of embeddingParameterM and delayParameterV");
//
//            globalResultStringBuilder.append("\nFile "
//                    + "\"" + fileName + "\""
//                    + " is skipped since the user it belongs to has the lowest frequent visit history"
//                    + "\nwith maximum possible K for predicting"
//                    + "\nnext K visits = "
//                    + "(lowestVisitHistoryTimeSeriesSize - 1) - (embeddingParameterM - 1) * delayParameterV = "
//                    + maxKOfTheLowestFrequentVisitHistoryOfChosenVisitHistories
//                    + "\nwhich cannot be 0 or negative"
//                    + "\nplease try different combination of embeddingParameterM and delayParameterV\n");
//
//            //do not perform further calculations; return from the method
//            return globalResultStringBuilder.toString();
//        } // if
//
//        */  // LESS RELIABLE METHOD
//
//
//
//
//
//        //------------------------------------------------------------------------------------------
//        //create an instance of NextPlace with embedding parameter m and delay parameter v
//        //and k for prediction.
//        //the given value of k for prediction will be adjusted based on the given configuration
//        //of the algorithm
//        NextPlace algorithm = new NextPlace(npConf);
//
//
//        // to test datasets for predictability error skipping predictions (chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations() method),
//        // to test one user
//        //if (mergedVisits.size() >= 0) return;
//
//
//
//        //create global set of all predicted visits
//        //this will be a mixed set of predictions to different locations
//        //sorted in chronological order;
//        //as stated by the algorithm k always starts from 1
//        TreeSet<Visit> globalSequenceOfPredictedVisits
//            = chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations(1, algorithm, visitHistories);
//
//
//        // returned globalSequenceOfPredictedVisits may have 0 size, handle it
//
//        //System.out.println("======= Global sequence of all predicted visits =======");
//        //for(Visit v : globalSequenceOfPredictedVisits)
//        //    System.out.println(v.toStringWithDate());
//        //make one more space
//        //System.out.println();
//
//
//        globalResultStringBuilder.append("\n======= Global sequence of all predicted visits" +
//                " in chronological order; #Predicted Visits: " + globalSequenceOfPredictedVisits.size() + "  =======\n");
//        for (Visit v : globalSequenceOfPredictedVisits)
//            globalResultStringBuilder.append(v.toStringWithDate() + "\n");
//        //make one more space
//        globalResultStringBuilder.append("\n");
//        //------------------------------------------------------------------------------------------
//
//
//        //get the predicted visit at timeT + deltaT
//        //as stated by the algorithm k always starts from 1
//        Visit predictedVisitAtTimeTPlusDeltaT
//                = algorithm.predictNextVisitAtTimeTPlusDeltaT(1,
//                                                                globalResultStringBuilder,
//                                                                globalSequenceOfPredictedVisits, visitHistories);
//
//        //get timeT and deltaT from NextPlaceConfiguration instance
//        long timeTInDaySeconds = npConf.getTimeTInDaySeconds();
//        long deltaTSeconds = npConf.getDeltaTSeconds();
//
//        //if predicted visit is non-null, write the details string builder
//        if (predictedVisitAtTimeTPlusDeltaT != null)
//        {
//            //System.out.println("\nThe user will make this visit: "
//            //        + predictedVisitAtTimeTPlusDeltaT.toStringWithDate()
//            //        + "\nat timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400)
//            //        + "\nto the location: " + predictedVisitAtTimeTPlusDeltaT.getLocation());
//            globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");
//            globalResultStringBuilder.append("The user will make this visit: "
//                    + predictedVisitAtTimeTPlusDeltaT.toStringWithDate()
//                    + "\nat timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400)
//                    + "\nto the location: " + predictedVisitAtTimeTPlusDeltaT.getLocation() + "\n");
//            globalResultStringBuilder.append("-----------------------------------------------------------------------\n");
//        }
//        else
//        {
//            //System.out.println("No location is predicted around day seconds"
//            //        + " timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400));
//
//            globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");
//            globalResultStringBuilder.append("No location is predicted around day seconds"
//                    + " timeT + deltaT = " + ((timeTInDaySeconds + deltaTSeconds) % 86400) + "\n");
//            globalResultStringBuilder.append("-----------------------------------------------------------------------\n");
//        } // else
//
//
//        //return the results as a string
//        return globalResultStringBuilder.toString();
//    } // resultsFromNextPlaceRun


    //helper method to get the file name (base name + extension) from the given path
    public static String fileNameFromPath(String path) {
        //Apache IO fileNameUtils works on every path
        //Tested on:
        //-----------------------------------------------
        //"hdfs://123.23.12.4344:9000/user/filename.txt"
        //"resources/cabs.xml"
        //"bluebird:/usr/doctools/dp/file.txt"
        //"\\\\server\\share\\file.xml"
        //"file:///C:/Users/Default/Downloads/file.pdf"
        //-----------------------------------------------
        //returns empty string for not found file names e.g. for => "\\\\server\\share\\"

        return FilenameUtils.getName(path);
    } // fileNameFromPath


    //helper method to find size of lowest-sized time series from visit histories
    private static int lowestTimeSeriesSizeOfAllCreatedVisitHistories(Collection<VisitHistory> visitHistories) {
        int lowestVisitHistoryTimeSeriesSize = 1000000000;

        if (visitHistories.size() == 0) return 0;

        //find a minimum
        for (VisitHistory visitHistory : visitHistories) {
            int visitHistoryTimeSeriesSize = visitHistory.getTimeSeries().size();
            if (visitHistoryTimeSeriesSize <= lowestVisitHistoryTimeSeriesSize) {
                lowestVisitHistoryTimeSeriesSize = visitHistoryTimeSeriesSize;
            } // if
        } // for

        return lowestVisitHistoryTimeSeriesSize;
    } //lowestTimeSeriesSizeOfCreatedVisitHistories


    //helper method to generate merged visits from file contents
    public static ArrayList<Visit> visitsFromPPUserFileContents(String fileContents) {
        //obtain merged visits by scanning file
        ArrayList<Visit> mergedVisits = new ArrayList<Visit>();
        Scanner scanner = null;

        try {
            //obtain scanner for file contents string
            scanner = new Scanner(fileContents);

            //now read the fileContents line by line
            while (scanner.hasNextLine()) {
                //read the fileContents line by line
                String line = scanner.nextLine();

                //the line structure is the following:
                //"username, latitude,longitude,unixTimeStamp,residenceTimeInSeconds" => which is actually a merged visit
                //we will do comma split
                String[] lineComponents = line.split(",");

                //get userName, locationName, latitude, longitude, arrivalTime as Date and residenceSeconds
                String userName = lineComponents[0];
                String locationName = lineComponents[1];
                double latitude = Double.parseDouble(lineComponents[2]);
                double longitude = Double.parseDouble(lineComponents[3]);
                long unixTimeStampInMilliseconds = Long.parseLong(lineComponents[4]);
                long residenceTimeInSeconds = Long.parseLong(lineComponents[5]);

                //create objects
                Location location = new Location(locationName, latitude, longitude);
                Date arrivalTime = new Date(unixTimeStampInMilliseconds);
                TimeSeriesInstant instant = new TimeSeriesInstant(arrivalTime, residenceTimeInSeconds);
                Visit mergedVisit = new Visit(userName, location, instant);

                //update the array list
                mergedVisits.add(mergedVisit);
            } // while

        } // try
        catch (Exception ex) {
            ex.printStackTrace();

            //if any other problem occurs during string scanning, return from method with empty list
            return new ArrayList<Visit>();
        } // catch
        finally {
            if (scanner != null) scanner.close();
        } // finally


        //return merged visits at the end, if the fileContents string is empty we will get empty list
        return mergedVisits;
    } // visitsFromPPUserFileContents


    ////helper method to get merged visits from pre-processed local file or network file input stream
    ////public static ArrayList<Visit> visitsFromPPUserFileInputStream(InputStream ppUserFileInputStream)
    //{
    //    //if some problems occurs in stringContentFromInputStream, we will get empty string;
    //    //which will yield an empty string in visitsFromPPUserFileContents
    //    String fileContents = stringContentFromInputStream(ppUserFileInputStream);
    //    return visitsFromPPUserFileContents(fileContents);
    //} // visitsFromPPUserFileInputStream


    public static ArrayList<Visit> visitsFromPPUserFileInputStream(InputStream ppUserFileInputStream) {
        //merged visits from input stream
        ArrayList<Visit> mergedVisits = new ArrayList<>();


        //buffered reader to read input stream
        BufferedReader br = null;

        try {
            //instead of reading whole file input stream at once, we read the file input stream character by character
            //create a buffered reader from input stream via input stream reader
            br = new BufferedReader(new InputStreamReader(ppUserFileInputStream, StandardCharsets.UTF_8));

            String line = null;
            while ((line = br.readLine()) != null) {
                //the line structure is the following:
                //"username, latitude,longitude,unixTimeStamp,residenceTimeInSeconds" => which is actually a merged visit
                //we will do comma split
                String[] lineComponents = line.split(",");

                //get userName, locationName, latitude, longitude, arrivalTime as Date and residenceSeconds
                String userName = lineComponents[0];
                String locationName = lineComponents[1];
                double latitude = Double.parseDouble(lineComponents[2]);
                double longitude = Double.parseDouble(lineComponents[3]);
                long unixTimeStampInMilliseconds = Long.parseLong(lineComponents[4]);
                long residenceTimeInSeconds = Long.parseLong(lineComponents[5]);

                //create objects
                Location location = new Location(locationName, latitude, longitude);
                Date arrivalTime = new Date(unixTimeStampInMilliseconds);
                TimeSeriesInstant instant = new TimeSeriesInstant(arrivalTime, residenceTimeInSeconds);
                Visit mergedVisit = new Visit(userName, location, instant);

                //update the array list
                mergedVisits.add(mergedVisit);
            } // while
        } // try
        catch (Exception ex) // will catch all exceptions including "IOException"
        {
            ex.printStackTrace();
        } // catch
        finally {
            //close buffered reader
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } // catch
            } // if

            if (ppUserFileInputStream != null) {
                try {
                    ppUserFileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } // if
        } // finally

        return mergedVisits;
    } // visitsFromPPUserFileInputStream


    public static ArrayList<Location> visitLocationsFromPPUserFileInputStream(InputStream ppUserFileInputStream) {
        ArrayList<Location> visitLocations = new ArrayList<>();

        //buffered reader to read input stream
        BufferedReader br = null;

        try {
            //instead of reading whole file input stream at once, we read the file input stream character by character
            //create a buffered reader from input stream via input stream reader
            br = new BufferedReader(new InputStreamReader(ppUserFileInputStream, StandardCharsets.UTF_8));

            String line = null;
            while ((line = br.readLine()) != null) {
                //the line structure is the following:
                //"username, latitude,longitude,unixTimeStamp,residenceTimeInSeconds" => which is actually a merged visit
                //we will do comma split
                String[] lineComponents = line.split(",");

                //get userName, locationName, latitude, longitude, arrivalTime as Date and residenceSeconds
                //String userName = lineComponents[0];
                String locationName = lineComponents[1];
                double latitude = Double.parseDouble(lineComponents[2]);
                double longitude = Double.parseDouble(lineComponents[3]);
                //long unixTimeStampInMilliseconds = Long.parseLong(lineComponents[4]);
                //long residenceTimeInSeconds = Long.parseLong(lineComponents[5]);

                //create objects
                Location location = new Location(locationName, latitude, longitude);
                //Date arrivalTime = new Date(unixTimeStampInMilliseconds);
                //TimeSeriesInstant instant = new TimeSeriesInstant(arrivalTime, residenceTimeInSeconds);
                //Visit mergedVisit = new Visit(userName, location, instant);

                //update the array list
                //mergedVisits.add(mergedVisit);
                visitLocations.add(location);
            } // while
        } // try
        catch (Exception ex) // will catch all exceptions including "IOException"
        {
            ex.printStackTrace();
        } // catch
        finally {
            //close buffered reader
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } // catch
            } // if

            if (ppUserFileInputStream != null) {
                try {
                    ppUserFileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } // if
        } // finally

        return visitLocations;
    } // visitLocationsFromPPUserFileInputStream


    private static String getFileExtension(File file) {
        String fileName = file.getName();
        if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
            return fileName.substring(fileName.lastIndexOf(".") + 1);
        else return "";
    } //getFileExtension


    //helper method to get maximum value of k for predicting future visits
    public static int maxValueOfFutureStepK(NextPlaceConfiguration npConf, int visitFrequency) {
        //visitFrequency - (embeddingParameterM - 1) * delayParameterV is the size of embedding space
        //there last embedding vector's index will be size of embedding space - 1;
        //maxK is always equal to size of embedding space - 1
        return (visitFrequency - 1) - (npConf.getEmbeddingParameterM() - 1) * npConf.getDelayParameterV();
    } // maxValueOfFutureStepK


    //method to find number of seconds between two dates
    public static long noOfSecondsBetween(Date dateOne, Date dateTwo) {
        //number of seconds between two dates
        long seconds;

        //If you have two Dates you can call getTime on them to get milliseconds, get the difference and divide by 1000
        if (dateOne.before(dateTwo)) {
            seconds = (dateTwo.getTime() - dateOne.getTime()) / 1000;
        } // if
        else if (dateOne.after(dateTwo)) {
            seconds = (dateOne.getTime() - dateTwo.getTime()) / 1000;
        } // else if
        else // two dates are equal
        {
            seconds = 0;
        } // else

        return seconds;
    } // noOfSecondsBetween


    //appropriate way to add seconds to a Date
    public static Date addSeconds(Date date, long seconds) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, (int) seconds);

        //.add() does not change the original <Date date> instance, therefore new Date should be returned
        return cal.getTime();
    } // addSeconds


    //appropriate way to subtract seconds from a Date
    public static Date subtractSeconds(Date date, long seconds) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, (int) (-seconds)); // subtract is like adding negative seconds

        //.add() does not change the original <Date date> instance, therefore new Date should be returned
        return cal.getTime();
    } // subtractSeconds


    //for setting hours, minutes, seconds of a date
    //hours are between 0 to 23, minutes are between 0 and 59, and seconds are between 0 and 59.
    public static Date getDate(Date date, int hours, int minutes, int seconds) {
        //Deprecated Date.getHours() returns the hour represented by this Date object.
        //The returned value is a number >> (0 through 23) << representing the hour within the day that
        //contains or begins with the instant in time represented by this Date object, as interpreted in the local time zone.
        //Calendar.HOUR_OF_DAY => Field number for get and set indicating the hour of the day.
        //HOUR_OF_DAY is used for the >> 24-hour clock. << E.g., at 10:04:15.250 PM the HOUR_OF_DAY is >> 22. <<

        //Deprecated Date.getMinutes() returns the number of minutes past the hour represented by this date, as interpreted in the local time zone.
        //The value returned is between >> 0 and 59. <<
        //Calender.MINUTE is the same.

        //Deprecated Date.getSeconds() Returns the number of seconds past the minute represented by this date.
        //The value returned is between >> 0 and 61. << The values 60 and 61 can only occur on those
        //Java Virtual Machines that take leap seconds into account.


        //Also, if you need to set timezone, add TimeZone tz = TimeZone.getTimeZone("UTC");
        // ... init calendar calendar.setTimeZone(tz);


        Calendar c = Calendar.getInstance();
        c.setTime(date);
        //c.set(Calendar.DATE, 2); // This is a synonym for DAY_OF_MONTH. The first day of the month has value 1.
        c.set(Calendar.HOUR_OF_DAY, hours);
        c.set(Calendar.MINUTE, minutes);
        c.set(Calendar.SECOND, seconds);
        //c.set(Calendar.MILLISECOND, 0);


        //return new Date
        return c.getTime();
    } // getDate


    //overloaded getDate method which creates a new Date with the given arrivalTimeInDaySeconds
    //by keeping its hours, minutes and seconds the same
    //arrivalTimeInDaySeconds will be converted to hours minutes and seconds
    public static Date getDate(Date date, long arrivalTimeInDaySeconds) {
        //obtain hours, minutes and seconds from daySeconds
        int hours = (int) (arrivalTimeInDaySeconds / 3600);
        int minutes = (int) ((arrivalTimeInDaySeconds % 3600) / 60);
        int seconds = (int) ((arrivalTimeInDaySeconds % 3600) % 60);

        //now call overloaded method which accepts hours, minutes and seconds
        return getDate(date, hours, minutes, seconds);
    } // getDate


    //helper method to get hours
    //values are returned between 0 and 23.
    public static int getHours(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    } // getHours

    //returned minutes are between 0 and 59.
    public static int getMinutes(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    } // getMinutes


    //returned seconds are between 0 and 59.
    public static int getSeconds(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    } // getSeconds


    //get a unix epoch date by the given arrival time in day seconds
    //day seconds are within the interval of [0, 86400]
    public static Date unixEpochDateWithDaySeconds(long daySeconds) {
        Calendar calendar = Calendar.getInstance();
        //Sets all the calendar field values and the time value (millisecond offset from the Epoch) of this Calendar undefined.
        //if we return calendar.getTime() right after clear() we will get =>  Thu Jan 01 00:00:00 AZT 1970
        calendar.clear();

        //obtain hours, minutes and seconds from daySeconds
        int hours = (int) (daySeconds / 3600);
        int minutes = (int) ((daySeconds % 3600) / 60);
        int seconds = (int) ((daySeconds % 3600) % 60);

        calendar.set(Calendar.HOUR_OF_DAY, hours);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);

        //return unix epoch date with the given day seconds converted to hours, minutes and seconds
        return calendar.getTime();
    } // unixEpochDateWithDaySeconds


    //method to find whether two dates are on the same day
    public static boolean onTheSameDay(Date dateOne, Date dateTwo) {
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        cal1.setTime(dateOne);
        cal2.setTime(dateTwo);
        return cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
                cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR);

        //return dateOne.getYear() == dateTwo.getYear()
        //        && dateOne.getMonth() == dateTwo.getMonth()
        //        && dateOne.getDay() == dateTwo.getDay();
    } // onTheSameDay


    //helper method to find the lowest visit frequency of the user when all visited locations are considered
    private static int lowestVisitFrequencyOfUser(ArrayList<Visit> visits) {
        // null check and isEmpty check
        if (visits == null || visits.isEmpty()) {
            System.err.println("visits is either null or empty; returning 0 as a lowestVisitFrequency");
            return 0;
        } // if

        int lowestFrequency = (int) Double.POSITIVE_INFINITY;

        //hash multi set is efficient in terms of frequency (count) calculation which is O(1)
        HashMultiset<Location> locations = locationHashMultiset(visits);
        //elementSet() returns the set of distinct elements contained in this multiset.
        //The element set is backed by the same data as the multiset, so any change to either is immediately reflected in the other.
        //The order of the elements in the element set is unspecified.
        for (Location uniqueLocation : locations.elementSet()) {
            //count or frequency is found in O(log(n)) time
            int frequency = locations.count(uniqueLocation);

            //if frequency is bigger than the highest found frequency
            //adjust highest frequency accordingly
            if (frequency < lowestFrequency)
                lowestFrequency = frequency;
        } // for

        return lowestFrequency;
    } //


    //helper method to find the highest visit frequency of the user when all visited locations are considered
    private static int highestVisitFrequencyOfUser(ArrayList<Visit> visits) {
        // null check and isEmpty check
        if (visits == null || visits.isEmpty()) {
            System.err.println("visits is either null or empty; returning 0 as a highestVisitFrequency");
            return 0;
        } // if


        //at this point there is at least one visit in visits array list
        //default frequency is 1; because user has visited at least one location
        int highestFrequency = 1;


        //hash multi set is efficient in terms of frequency (count) calculation which is O(1)
        HashMultiset<Location> locations = locationHashMultiset(visits);
        //elementSet() returns the set of distinct elements contained in this multiset.
        //The element set is backed by the same data as the multiset, so any change to either is immediately reflected in the other.
        //The order of the elements in the element set is unspecified.
        for (Location uniqueLocation : locations.elementSet()) {
            //count or frequency is found in O(log(n)) time
            int frequency = locations.count(uniqueLocation);

            //if frequency is bigger than the highest found frequency
            //adjust highest frequency accordingly
            if (frequency > highestFrequency)
                highestFrequency = frequency;
        } // for

        return highestFrequency;
    } // highestVisitFrequencyOfUser


    //overloaded method which finds the highest visit frequency of a user from its visit histories
    //it will basically return the frequency of the highest frequent visit history (the size of max-sized visit history)
    private static int highestVisitFrequencyOfUser(Collection<VisitHistory> visitHistories) {
        //default 1; because user has visited at least one location (which corresponds only visit history having only one visit)
        int highestFrequency = 1;

        //loop through all visit histories and find the highest frequent visit history
        for (VisitHistory visitHistory : visitHistories) {
            int frequency = visitHistory.getFrequency();

            //if frequency is bigger than the highest found frequency
            //adjust highest frequency accordingly
            if (frequency > highestFrequency)
                highestFrequency = frequency;
        } // for

        return highestFrequency;
    } // highestVisitFrequencyOfUser


    //helper method to obtain maxK of highest frequent visit history
    public static int maxKOfHighestFrequentVisitHistoryOfUser(NextPlaceConfiguration npConf, Collection<VisitHistory> visitHistories) {
        return maxValueOfFutureStepK(npConf, highestVisitFrequencyOfUser(visitHistories));
    } // maxKOfHighestFrequentVisitHistoryOfUser


    //overloaded method which creates visit histories, with frequency threshold 1;
    //there is no need to calculate locationAndFrequencyMap beforehand;
    //so, it saves run time
    public static Collection<VisitHistory> createVisitHistories(ArrayList<Visit> mergedVisits)
    {
        //we assume that merged visits are chronologically ordered


        //visit histories for all visits will be created, even for locations which are visited only once
        HashMap<Location, VisitHistory> locationVisitHistoryHashMap = new HashMap<Location, VisitHistory>();
        for (Visit v : mergedVisits) {
            Location location = v.getLocation();
            if (locationVisitHistoryHashMap.containsKey(location)) {
                VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                locationVisitHistory.add(v);
            } // if
            else {
                VisitHistory newVisitHistory = new VisitHistory();
                newVisitHistory.add(v);
                locationVisitHistoryHashMap.put(location, newVisitHistory);
            } // else

        } // for

        return locationVisitHistoryHashMap.values();
    } // createVisitHistories


    //helper method to create visit histories from merged visits, where visit count for the specific
    //location should not be smaller than frequency threshold
    public static Collection<VisitHistory> createVisitHistories(ArrayList<Visit> mergedVisits,
                                                                int frequencyThreshold) {
        //obtain location and frequency map
        //HashMap<Location, Integer> locationAndFrequencyMap = locationAndFrequencyMap(mergedVisits);

        //obtain location tree multi-set
        HashMultiset<Location> locationHashMultiset = locationHashMultiset(mergedVisits);

        //obtain location and frequency entry set
        //Set<Multiset.Entry<Location>> locationAndFrequencyEntrySet = locationAndFrequencyEntrySet(mergedVisits);

        //NEW METHOD
        HashMap<Location, VisitHistory> locationVisitHistoryHashMap = new HashMap<Location, VisitHistory>();
        for (Visit v : mergedVisits) {
            Location location = v.getLocation();
            //obtain the frequency, then check against certain threshold; hash multi set count is O(1)
            int frequency = locationHashMultiset.count(location); //locationAndFrequencyMap.get(location);
            if (frequency >= frequencyThreshold) {
                if (locationVisitHistoryHashMap.containsKey(location)) {
                    VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                    locationVisitHistory.add(v);
                } // if
                else {
                    VisitHistory newVisitHistory = new VisitHistory();
                    newVisitHistory.add(v);
                    locationVisitHistoryHashMap.put(location, newVisitHistory);
                } // else
            } // if
        } // for

        return locationVisitHistoryHashMap.values();
    } //createVisitHistories


    //helper method to create visit histories such that K elements can be chosen for the test split
    public static Collection<VisitHistory> createVisitHistories(ArrayList<Visit> visits,
                                                                NextPlaceConfiguration npConf,
                                                                int kElementsForTest) {
        assert kElementsForTest >= 1;

        //int lowestFrequency = lowestVisitFrequencyOfUser(visits);
        //if(kElementsForTest > maxValueOfFutureStepK(npConf, lowestFrequency - kElementsForTest))
        //    throw new RuntimeException("Pick smaller K elements to choose for a test split");


        //obtain location hash multi-set
        HashMultiset<Location> locationHashMultiset = locationHashMultiset(visits);


        //NEW METHOD
        HashMap<Location, VisitHistory> locationVisitHistoryHashMap = new HashMap<Location, VisitHistory>();
        for (Visit v : visits) {
            Location location = v.getLocation();
            //obtain the frequency, then check against certain threshold
            //frequency (count) check is just O(1) in hash multi set
            int frequency = locationHashMultiset.count(location);

            //obtain locations whose predictability rate (maxK) is at least one
            //this will eliminate locations which is not predictable
            //meaning that only predictable visit histories will be created based on npConf;
            //For division into two parts:
            //1) frequency should be at least 3
            //2) visit history should be predictable which means maxK should be at least one
            //3) first part should be predictable
            if (frequency >= 3 && maxValueOfFutureStepK(npConf, frequency - kElementsForTest) >= kElementsForTest) {
                //above check will ensure => f >= 2k + 1 => which will ensure that first part f - k >= k + 1
                //such that the first part will be predictable
                if (locationVisitHistoryHashMap.containsKey(location)) {
                    VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                    locationVisitHistory.add(v);
                } // if
                else {
                    VisitHistory newVisitHistory = new VisitHistory();
                    newVisitHistory.add(v);
                    locationVisitHistoryHashMap.put(location, newVisitHistory);
                } // else
            } // if

        } // for

        return locationVisitHistoryHashMap.values();
    } // createVisitHistories


    //helper method to create predictable visit histories based on given train and test split
    public static Collection<VisitHistory> createVisitHistories(ArrayList<Visit> visits,
                                                                NextPlaceConfiguration npConf,
                                                                double trainSplitFraction,
                                                                double testSplitFraction) {
        //ensure correctness of fractions first
        ensureCorrectnessOfFractions(trainSplitFraction, testSplitFraction);

        //obtain location hash multi-set
        HashMultiset<Location> locationHashMultiset = locationHashMultiset(visits);

        //NEW METHOD
        HashMap<Location, VisitHistory> locationVisitHistoryHashMap = new HashMap<Location, VisitHistory>();
        for (Visit v : visits) {
            Location location = v.getLocation();
            //obtain the frequency, then check against certain threshold
            //frequency (count) check is just O(1) in hash multi set
            int frequency = locationHashMultiset.count(location);

            //obtain locations whose predictability rate (maxK) is at least one
            //this will eliminate locations which is not predictable
            //meaning that only predictable visit histories will be created based on npConf;
            //For division into two parts:
            //1) frequency should be at least 3
            //2) visit history should be predictable which means maxK should be at least one
            //3) first part should be predictable
            if (frequency >= 3 && maxValueOfFutureStepK(npConf, frequency) >= 1) {
                //calculate the length of first and second part
                int lengthOfSecondPart = (int) (frequency * testSplitFraction);
                if (lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
                int lengthOfFirstPart = frequency - lengthOfSecondPart;

                //if by integer division accidentally first part and second part have same size, make second part one smaller
                //and first part one bigger
                if (lengthOfFirstPart == lengthOfSecondPart) {
                    lengthOfSecondPart -= 1;
                    lengthOfFirstPart += 1;
                } // if


                //if the first part is predictable, generate a visit history
                if (Utils.maxValueOfFutureStepK(npConf, lengthOfFirstPart) >= 1) {
                    if (locationVisitHistoryHashMap.containsKey(location)) {
                        VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                        locationVisitHistory.add(v);
                    } // if
                    else {
                        VisitHistory newVisitHistory = new VisitHistory();
                        newVisitHistory.add(v);
                        locationVisitHistoryHashMap.put(location, newVisitHistory);
                    } // else
                } // if
            } // if

        } // for

        return locationVisitHistoryHashMap.values();
    } // createVisitHistories


    //Overloaded helper method to create visit histories from merged visits
    //this method eliminates every visit, whose location's predictability rate (maxK) is smaller or equal to 0
    //as compared to frequency threshold method, this method is more reliable
    //since, only visit histories which are predictable are created
    //and there is no need to re-check maxK of lowest frequent visit history of the created visit histories
    public static Collection<VisitHistory> createVisitHistories(ArrayList<Visit> mergedVisits,
                                                                NextPlaceConfiguration npConf,
                                                                boolean halfDividable) {
        //obtain location and frequency map
        //HashMap<Location, Integer> locationAndFrequencyMap = locationAndFrequencyMap(mergedVisits);

        //obtain location hash multi-set
        HashMultiset<Location> locationHashMultiset = locationHashMultiset(mergedVisits);

        //obtain location and frequency entry set
        //Set<Multiset.Entry<Location>> locationAndFrequencyEntrySet = locationAndFrequencyEntrySet(mergedVisits);


        //NEW METHOD
        HashMap<Location, VisitHistory> locationVisitHistoryHashMap = new HashMap<Location, VisitHistory>();
        for (Visit v : mergedVisits) {
            Location location = v.getLocation();
            //obtain the frequency, then check against certain threshold
            //frequency (count) check is just O(1) in hash multi set
            int frequency = locationHashMultiset.count(location); //locationAndFrequencyMap.get(location);

            //obtain locations whose predictability rate (maxK) is at least one
            //this will eliminate locations which is not predictable
            //meaning that only predictable visit histories will be created based on npConf
            if (maxValueOfFutureStepK(npConf, frequency) >= 1) {
                //increment predictable locations count
                //locationCount++;
                //System.out.println(locationCount + ": " + location + " => " + frequency);


                //if visit history's time series is intended to be divided into two parts to predict second half
                //from the first half;
                //the number of elements in original time series (frequency) should be at least 3
                //and original time series should be predictable (which is done in above if) which means frequency >= 3
                if (halfDividable) {
                    if (frequency >= 3) {
                        //regardless of frequency is an odd or even number, after division length (frequency) of the first half
                        //will be calculated using the same way
                        int lengthOfFirstHalf = (frequency / 2) + 1;
                        //now if the first half is predictable, then create a visit history
                        if (maxValueOfFutureStepK(npConf, lengthOfFirstHalf) >= 1) {
                            if (locationVisitHistoryHashMap.containsKey(location)) {
                                VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                                locationVisitHistory.add(v);
                            } // if
                            else {
                                VisitHistory newVisitHistory = new VisitHistory();
                                newVisitHistory.add(v);
                                locationVisitHistoryHashMap.put(location, newVisitHistory);
                            } // else
                        } // if

                    } // if

                } // if
                else {
                    if (locationVisitHistoryHashMap.containsKey(location)) {
                        VisitHistory locationVisitHistory = locationVisitHistoryHashMap.get(location);
                        locationVisitHistory.add(v);
                    } // if
                    else {
                        VisitHistory newVisitHistory = new VisitHistory();
                        newVisitHistory.add(v);
                        locationVisitHistoryHashMap.put(location, newVisitHistory);
                    } // else
                } // else
            } // if


        } // for

        return locationVisitHistoryHashMap.values();
    } // createVisitHistories


    //helper method to ensure fractions adds up to one and test split fraction is not bigger than train split fraction
    private static void ensureCorrectnessOfFractions(double trainSplitFraction, double testSplitFraction) {
        if (Double.compare(trainSplitFraction + testSplitFraction, 1.0) != 0)
            throw new RuntimeException("Train and test split fractions must add up to 1");

        if (testSplitFraction > trainSplitFraction)
            throw new RuntimeException("Test split fraction cannot be bigger than train split fraction");
    } // ensureCorrectnessOfFractions


    //helper method to generate location and frequency map of merged visits
    private static HashMap<Location, Integer> locationAndFrequencyMap(ArrayList<Visit> mergedVisits) {
        //array list to hold locations from merged visits in the same order
        //ArrayList<Location> locations = new ArrayList<Location>();
        //hash set to hold unique locations
        //HashSet<Location> uniqueLocations = new HashSet<Location>();


        //Multisets (also known as Bags), is a Set that allows duplicate elements;
        //TreeMultiSet which maintains the ordering of its elements, according to either their natural order or an explicit Comparator.
        //In all cases, this implementation uses Comparable.compareTo(T) or Comparator.compare(T, T)
        //instead of Object.equals(java.lang.Object) to determine equivalence of instances;
        //count or frequency of an element can be obtained in O(log(n)) steps instead of O(n), where n is the size of the collection.
        HashMultiset<Location> locations = HashMultiset.create(); // For a HashMultiset, count is O(1), for a TreeMultiset, count is O(log n)


        //Location can be a key in a map, rather than Visit itself
        //therefore we will have the locations array list
        //as in the order of merged visits
        for (Visit v : mergedVisits) {
            locations.add(v.getLocation());

            //add() method only returns true if the hash set (uniqueLocations) did not already
            //contain the specified element (and adds it to the set)
            //uniqueLocations.add(v.getLocation()); // adding to the hash set takes O(1) time
        } // for

        //we will obtain number of unique locations those are visited 1 or more times
        //creating Map from locations is reasonable, since equality of visits
        //are determined not only by location but also by time series instant
        HashMap<Location, Integer> locationAndFrequencyMap = new HashMap<Location, Integer>();


        //1) Collections.frequency() => runtime is bounded by O(n^2), worst case happens when each visit is performed to the unique location
        //in mergedVisits array list(#visits == #unique locations), which means locations.size() and uniqueLocations.size() will be the same;
        //2) TreeMultiSet.count() => runtime is bounded by O(n * log(n)), where n is the same as above argument,
        //but frequency is found in O(log(n)) time;

        //elementSet() returns the set of distinct elements contained in this multiset.
        //The element set is backed by the same data as the multiset, so any change to either is immediately reflected in the other.
        //The order of the elements in the element set is unspecified.
        for (Location uniqueLocation : locations.elementSet()) //uniqueLocations)
        {
            //Collections.frequency has O(n) runtime for each unique element in the for loop definition;
            //but locations.count(uniqueLocation) will use O(log(n)) time since, tree multi set is already sorted
            locationAndFrequencyMap.put(uniqueLocation, locations.count(uniqueLocation));

            //locationAndFrequencyMap.put(uniqueLocation, Collections.frequency(locations, uniqueLocation));
        } // for

        return locationAndFrequencyMap;
    } // locationAndFrequencyMap


    //helper method to get the location tree multi-set's entry set, which contains unique locations and their frequencies (counts)
    private static Set<Multiset.Entry<Location>> locationAndFrequencyEntrySet(ArrayList<Visit> mergedVisits) {
        //Multisets (also known as Bags), is a Set that allows duplicate elements;
        //TreeMultiSet which maintains the ordering of its elements, according to either their natural order or an explicit Comparator.
        //In all cases, this implementation uses Comparable.compareTo(T) or Comparator.compare(T, T)
        //instead of Object.equals(java.lang.Object) to determine equivalence of instances;
        //count or frequency of an element can be obtained in O(log(n)) steps instead of O(n), where n is the size of the collection.
        HashMultiset<Location> locations = HashMultiset.create();  // For a HashMultiset, count is O(1), for a TreeMultiset, count is O(log n)


        //Location can be a key in a map, rather than Visit itself
        //therefore we will have the locations array list
        //as in the order of merged visits
        for (Visit v : mergedVisits) {
            locations.add(v.getLocation());
        } // for

        //1) Collections.frequency() => runtime is bounded by O(n^2), worst case happens when each visit is performed to the unique location
        //in mergedVisits array list(#visits == #unique locations), which means locations.size() and uniqueLocations.size() will be the same;
        //2) TreeMultiSet.count() => runtime is bounded by O(n * log(n)), where n is the same as above argument,
        //but frequency is found in O(log(n)) time;
        return locations.entrySet();
    } // locationAndFrequencyEntrySet


    //helper method to get the location tree multi-set, which contains unique locations and their frequencies (counts)
    public static HashMultiset<Location> locationHashMultiset(ArrayList<Visit> visits) {
        //Multi sets (also known as Bags), is a Set that allows duplicate elements;
        //TreeMultiSet which maintains the ordering of its elements, according to either their natural order or an explicit Comparator.
        //In all cases, this implementation uses Comparable.compareTo(T) or Comparator.compare(T, T)
        //instead of Object.equals(java.lang.Object) to determine equivalence of instances;
        //count or frequency of an element can be obtained in O(log(n)) steps instead of O(n), where n is the size of the collection.
        HashMultiset<Location> locations = HashMultiset.create();  // For a HashMultiset, count is O(1), for a TreeMultiset, count is O(log n)


        //Location can be a key in a map, rather than Visit itself
        //therefore we will have the locations array list
        //as in the order of merged visits
        for (Visit v : visits) {
            locations.add(v.getLocation());
        } // for

        return locations;
    } // locationHashMultiset


    //helper method to generate sequence of predicted to different locations from "visitHistories"
    //sorted in chronological order
    public static TreeSet<Visit>
    chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations(int futureStepK,
                                                                        NextPlace algorithm,
                                                                        Collection<VisitHistory> visitHistories,
                                                                        //HashMap<Integer, Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>> vhAlphaGlobalMap,
                                                                        //HashMap<Integer, SetMultimap<Double, Integer>> vhDistIndexPairsMap,
                                                                        //HashMap<Integer, Double> vhStdMap
                                                                        HashMap<Integer, Integer> vhIndexOfBetaNMap,
                                                                        HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap
                                                                        )
    {
        //to check against locationCount
        int visitHistoryCount = 1;

        //create global set of all predicted visits
        //this will be a mixed set of predictions to different locations
        //sorted in chronological order
        TreeSet<Visit> setOfVisitsToDifferentLocations = new TreeSet<Visit>();

        //for simplicity create an embedding space from one visitHistory
        for (VisitHistory visitHistory : visitHistories)
        {
            Integer hashOfThisVH = visitHistory.hashCode();
            //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
            //        indexOfBeta_N_alphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfThisVH);
            //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
            int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfThisVH); //indexOfBeta_N_alphaGlobalTuple2._1;
            //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfThisVH);
            //Double stdOfThisVH = vhStdMap.get(hashOfThisVH);
            LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfThisVH);


//            //nIndex and distance pairs; distance is between betta_nIndex and betta_NIndex
//            //we will iterate through alphaGlobal.keySet() in natural order of keys,
//            //therefore for indexDistancePairs insertion order is enough,
//            //where get, put, remove and containsKey takes O(1) time
//            LinkedHashMap<Integer, Double> indexDistancePairs = new LinkedHashMap<Integer, Double>();
//
//            //iterate over all B_n in alphaGlobal,
//            //alphaGlobal contains keys sorted in ascending order
//            //therefore, indexDistancePairs keys will be in ascending order
//            for(Integer nIndex : alphaGlobal.keySet())
//            {
//                //index of B_N is ignored right after for loop
//                double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
//                indexDistancePairs.put(nIndex, distance);
//            } // for
//            //remove index of B_N from the map, since we find neighbors of B_N
//            indexDistancePairs.remove(indexOfBeta_N); //NIndex);
//
//            //sort map by ascending distance order
//            LinkedHashMap<Integer, Double> indexDistancePairsSortedByAscendingDistance = Utils.sortMapByValues(indexDistancePairs);


//            //now generate the neighborhood of B_N from embedding space
//            LinkedHashMap<Integer, EmbeddingVector> neighborhood
//                                = algorithm.generateNeighborhood(kForPredictingNextVisit, //NEW
//                                                                 timeSeriesOfThisVisitHistory,
//                                                                 alphaGlobal,
//                                                                 indexOfBeta_N,
//                                                                 distanceIndexPairs);
//                                                                  //indexDistancePairsSortedByAscendingDistance);
//                                                                  //indexDistancePairs);
//
//            //index of bette_N, will be used in NextPlace.predict() method
//            //int NIndex = alphaGlobal.lastKey();
//
//            /*
//            //print the relevant info
//            EmbeddingVector betta_N = alphaGlobal.get(NIndex);
//            System.out.println("======= B_" + NIndex + " =======");
//            //System.out.println(betta_N);
//            System.out.println(betta_N.toStringWithDate());
//
//            ///*
//            System.out.println("======= Neighborhood of B_" + NIndex + " =======");
//
//            //print out all embedding vectors from the neighborhood of B_N
//            for (Integer nIndex : neighborhood.keySet()) {
//                EmbeddingVector neighbor_betta_nIndex = neighborhood.get(nIndex);
//                System.out.println("======= B_" + nIndex + " =======");
//                //System.out.println(neighbor_betta_nIndex);
//                System.out.println(neighbor_betta_nIndex.toStringWithDate());
//
//                System.out.println("======= Distance between B_" + NIndex
//                        + " and B_" + nIndex + " =======");
//                System.out.println(betta_N.distanceTo(neighbor_betta_nIndex) + "\n");
//            } // for
//            */
//
//
//
//            //"considering new visits" means, when k = 2 (in case of a repeat of the algorithm),
//            //discard visits generated by k = 1, because they are already checked for location prediction;
//            //predict future visit which is kForPredictingNextVisit steps ahead from this visit history
//            Visit predictedVisit
//                            = algorithm.predictAVisitKStepsAhead(kForPredictingNextVisit, // NEW
//                                                                visitHistory.getUserName(),
//                                                                visitHistory.getLocation(),
//                                                                neighborhood, indexOfBeta_N, timeSeriesOfThisVisitHistory);


            //--------------------------- NEW -------------------------
            Visit predictedVisit = algorithm.predictAVisitKStepsAhead(futureStepK, visitHistory,
                    //stdOfThisVH,
                    //alphaGlobal,
                    neighborhood,
                    indexOfBeta_N);
                    //distanceIndexPairs);
            //indexDistancePairs);
            //1);
            //--------------------------- NEW -------------------------


            //it is possible that predicted visit is null, because of an empty-neighborhood;
            //then add non-null visits to the set
            if (predictedVisit != null)
                setOfVisitsToDifferentLocations.add(predictedVisit);


            //increment visitHistoryCount for next visit histories
            visitHistoryCount++;

            //process only one visit history
            //break; //perform NextPlace for all visit histories (locations)
        } // for

        //reset visit history count
        visitHistoryCount = 1;


        /*
        System.out.println("======= Global sequence of all predicted visits =======");
        for(Visit v : setOfVisitsToDifferentLocations)
            System.out.println(v.toStringWithDate());
        //make one more space
        System.out.println();
        */


        //return global sequence
        return setOfVisitsToDifferentLocations;
    } // chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations


    //helper method to calculate the predictability error of a dataset in a single machine mode
    public static double peOfADataset(Dataset dataset, List<String> ppLocalUserFilePaths,
                                      NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();

        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Calculating PE of the " + dataset + " .....");
        } // if


        //sum of PEs, initially 0
        double sumOfPEsOfUsers = 0;

        //loop through all preprocessed user files
        for (String ppLocalUserFilePath : ppLocalUserFilePaths) {
            //get the contents of the file from local file path
            String ppLocalUserFileContents = fileContentsFromLocalFilePath(ppLocalUserFilePath);

            //update the sum
            sumOfPEsOfUsers += peOfPPUserFile(ppLocalUserFileContents, npConf, trainSplitFraction, testSplitFraction);
        } // for

        //calculate PE_global which is the predictability error of the dataset
        double PE_global = sumOfPEsOfUsers / ppLocalUserFilePaths.size();

        long endTime = System.nanoTime();
        System.out.println("||-----------------------------------------------------------------------------------||");
        System.out.println("|| PE calculation of " + dataset + " took: " + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");

        return PE_global;
    } // peOfADataset


    //helper method to calculate PE of a dataset on files from the local directory path
    public static double peOfADataset(Dataset dataset, String localDirPathOfPPUserFiles, boolean recursive,
                                      NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction) {
        //get the files from given path; if the given path is actually a file, only the result for that file will be returned
        List<String> ppUserFilePaths = listFilesFromLocalPath(localDirPathOfPPUserFiles, recursive);

        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting list of files is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //since the directory is, there is nothing predictable; return 1.0
            return 1.0;
        } else    //paths contains at least one file
        {
            //return the result of overloaded method
            return peOfADataset(dataset, ppUserFilePaths, npConf, trainSplitFraction, testSplitFraction);
        } // else

    } // peOfADataset



//    public static Tuple2<Double, String> testPEOfADatasetDistributed(Dataset dataset, JavaSparkContext sc,
//                                                                     List<String> ppUserFilePaths,
//                                                                     int requiredPartitionCount,
//                                                                     NextPlaceConfiguration npConf,
//                                                                     double trainSplitFraction,
//                                                                     double testSplitFraction) {
//        //for finding distributed NextPlace run time
//        long startTime = System.nanoTime();
//
//        //print dataset info and s.o.p algorithm run message if not null
//        if (dataset != null) {
//            dataset.printInfo();
//            System.out.println("Distributed PE calculation of the " + dataset + " .....");
//        } // if
//
//
//        //will be used to get hadoop configuration inside transformation functions;
//        //since SparkContext is NOT usable inside transformation functions
//        final Broadcast<SerializableWritable<Configuration>> broadcastConf
//                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));
//
//        final Broadcast<Double> broadcastRangeInMetersToSmoothLocations = sc.broadcast(10.0);
//        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
//        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
//        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
//
//        //get rdd of preprocessed user file paths
//        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, requiredPartitionCount);
//
//        //prints the number of processors/workers or threads that spark will execute
//        System.out.println("\nDefault parallelism: " + sc.defaultParallelism());
//        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
//        System.out.println("Required partition count: " + requiredPartitionCount);
//        System.out.println("Partitions count of the created RDD: " + ppUserFilePathsRDD.partitions().size() + "\n");
//
//        JavaPairRDD<String, List<Visit>> userMergedVisitsPairRDD = ppUserFilePathsRDD.mapToPair(
//                new PairFunction<String, String, List<Visit>>() {
//                    @Override
//                    public Tuple2<String, List<Visit>> call(String ppUserFilePath) throws Exception {
//                        String fileContents = Utils.fileContentsFromHDFSFilePath(broadcastConf.value().value(),
//                                ppUserFilePath, false);
//
//                        return new Tuple2<String, List<Visit>>(ppUserFilePath, Utils.visitsFromPPUserFileContents(fileContents));
//                    } // call
//                });
//
//
//        JavaPairRDD<String, List<Location>> userUniqueLocationsRDD = userMergedVisitsPairRDD.mapToPair(
//                new PairFunction<Tuple2<String, List<Visit>>, String, List<Location>>() {
//                    @Override
//                    public Tuple2<String, List<Location>> call(Tuple2<String, List<Visit>> stringListTuple2) throws Exception {
//                        String ppUserFilePath = stringListTuple2._1;
//                        List<Visit> mergedVisits = stringListTuple2._2;
//                        HashSet<Location> uniqueLocations = new HashSet<Location>();
//                        for (Visit v : mergedVisits) {
//                            uniqueLocations.add(v.getLocation());
//                        } // for
//
//                        return new Tuple2<>(ppUserFilePath, new ArrayList<Location>(uniqueLocations));
//                    } // call
//                });
//
//
//        JavaPairRDD<String, List<Location>> userMergedLocationsRDD = userUniqueLocationsRDD.mapToPair(
//                new PairFunction<Tuple2<String, List<Location>>, String, List<Location>>() {
//                    @Override
//                    public Tuple2<String, List<Location>> call(Tuple2<String, List<Location>> stringListTuple2) throws Exception {
//                        String ppUserFilePath = stringListTuple2._1;
//                        List<Location> ul = stringListTuple2._2;
//                        HashMap<Integer, List<Location>> locationGroup = new HashMap<Integer, List<Location>>();
//                        for (int i = 0; i < ul.size(); i++) {
//                            for (int j = 0; j < ul.size(); j++) {
//                                double dist = ul.get(i).distanceTo(ul.get(j));
//
//                                if (dist < broadcastRangeInMetersToSmoothLocations.value()) //2.0f)
//                                {
//                                    if (locationGroup.containsKey(i)) {
//                                        locationGroup.get(i).add(ul.get(j));
//                                    } // if
//                                    else {
//                                        //list group will continue only unique locations, since ul conntains only unique locations
//                                        List<Location> group = new ArrayList<Location>();
//                                        group.add(ul.get(j));
//                                        locationGroup.put(i, group);
//                                    } // else
//                                } // if
//                            } // for
//                        } // for
//
//                        //ul.clear(); ul = null;
//
//                        HashSet<List<Location>> locationGroupSet = new HashSet<List<Location>>(locationGroup.values());
//                        locationGroup.clear();
//                        locationGroup = null;
//
//                        HashSet<Location> finalLocations = new HashSet<Location>();
//                        //hash set of averaged locations
//                        for (List<Location> locGroup : locationGroupSet) {
//                            double sumOfLatitudes = 0;
//                            double sumOfLongitudes = 0;
//
//                            //BigDecimal sumOfLatitudes = BigDecimal.ZERO;
//                            //BigDecimal sumOfLongitudes = BigDecimal.ZERO;
//
//                            //find average latitude
//                            for (Location location : locGroup) {
//                                sumOfLatitudes += location.getLatitude();
//                                sumOfLongitudes += location.getLongitude();
//                                //sumOfLatitudes = sumOfLatitudes.add(BigDecimal.valueOf(location.getLatitude()));
//                                //sumOfLongitudes = sumOfLongitudes.add(BigDecimal.valueOf(location.getLongitude()));
//                            } // for
//
//                            //find average
//                            double averageLat = //sumOfLatitudes.divide(BigDecimal.valueOf(locGroup.size()), BigDecimal.ROUND_UP).doubleValue();
//                                    sumOfLatitudes / locGroup.size();
//                            double averageLng = //sumOfLongitudes.divide(BigDecimal.valueOf(locGroup.size()), BigDecimal.ROUND_UP).doubleValue();
//                                    sumOfLongitudes / locGroup.size();
//
//                            //set the location name of the first location in a locGroup
//                            //to be location name of the averaged location
//                            finalLocations.add(new Location(locGroup.get(0).getName(), averageLat, averageLng));
//                        } // for
//
//                        locationGroupSet.clear();
//                        locationGroupSet = null;
//
//                        return new Tuple2<String, List<Location>>(ppUserFilePath, new ArrayList<Location>(finalLocations));
//                    } // call
//                });
//
//
//        JavaPairRDD<String, ArrayList<Visit>> userGauussianizedMergedVisitsPairRDD = userMergedLocationsRDD.mapToPair(new PairFunction<Tuple2<String, List<Location>>, String, ArrayList<Visit>>() {
//            @Override
//            public Tuple2<String, ArrayList<Visit>> call(Tuple2<String, List<Location>> stringListTuple2) throws Exception {
//                String ppUserFilePath = stringListTuple2._1;
//                List<Location> finalLocations = stringListTuple2._2;
//
//                String fileContents = Utils.fileContentsFromHDFSFilePath(broadcastConf.value().value(),
//                        ppUserFilePath, false);
//
//                ArrayList<Visit> modifiableMergedVisitList = new ArrayList<Visit>(Utils.visitsFromPPUserFileContents(fileContents));
//                Iterator<Visit> visitIterator = modifiableMergedVisitList.iterator();
//                ArrayList<Visit> gaussianizedMergedVisits = new ArrayList<Visit>();
//                while (visitIterator.hasNext()) {
//                    //int oldSize = gaussianizedMergedVisits.size();
//                    Visit v = visitIterator.next();
//
//                    for (Location location : finalLocations) {
//                        double dist = v.getLocation().distanceTo(location);
//                        if (dist < broadcastRangeInMetersToSmoothLocations.value()) //2.0f)
//                        {
//                            Visit newVisit = new Visit(v.getUserName(), location, v.getTimeSeriesInstant());
//                            gaussianizedMergedVisits.add(newVisit);
//                            visitIterator.remove();
//                            break;
//                        } // if
//                    } // for
//
//
//                } // for
//
//                modifiableMergedVisitList.clear();
//                modifiableMergedVisitList = null;
//
//                return new Tuple2<String, ArrayList<Visit>>(ppUserFilePath, gaussianizedMergedVisits);
//            } // call
//        });
//
//
//        JavaPairRDD<Double, String> PEsOfUsersResultStringPairRDD = userGauussianizedMergedVisitsPairRDD.mapToPair(
//                new PairFunction<Tuple2<String, ArrayList<Visit>>, Double, String>() {
//                    @Override
//                    public Tuple2<Double, String> call(Tuple2<String, ArrayList<Visit>> stringListTuple2) throws Exception {
//                        String ppUserFilePath = stringListTuple2._1;
//                        ArrayList<Visit> gaussianizedMergedVisits = stringListTuple2._2;
//                        double peOfUserFile = Utils.peOfPPUserFile(broadcastNpConf.value(), gaussianizedMergedVisits,
//                                broadcastTrainSplitFraction.value(), broadcastTestSplitFraction.value());
//                        String result = "PE of user at path: " + Utils.fileNameFromPath(ppUserFilePath) + " => " + peOfUserFile;
//                        return new Tuple2<Double, String>(peOfUserFile, result);
//                    } // call
//                });
//
//
//        Tuple2<Double, String> sum_of_PEs_And_GlobalStringTuple
//                = PEsOfUsersResultStringPairRDD.reduce(
//                new Function2<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>>() {
//                    @Override
//                    public Tuple2<Double, String> call(Tuple2<Double, String> doubleStringTupleOne, Tuple2<Double, String> doubleStringTupleTwo) throws Exception {
//                        double sum_of_PEs = doubleStringTupleOne._1 + doubleStringTupleTwo._1;
//                        String sumString = doubleStringTupleOne._2 + "\n" + doubleStringTupleTwo._2;
//                        return new Tuple2<>(sum_of_PEs, sumString);
//                    } // call
//                });
//
//
//        //unpersist rdds and broadcast conf; make them ready for GC
//        ppUserFilePathsRDD.unpersist();
//        userMergedVisitsPairRDD.unpersist();
//        userUniqueLocationsRDD.unpersist();
//        userMergedLocationsRDD.unpersist();
//        userGauussianizedMergedVisitsPairRDD.unpersist();
//        PEsOfUsersResultStringPairRDD.unpersist();
//        broadcastConf.unpersist();
//        broadcastRangeInMetersToSmoothLocations.unpersist();
//        broadcastTrainSplitFraction.unpersist();
//        broadcastTestSplitFraction.unpersist();
//        ppUserFilePathsRDD = null;
//        userMergedVisitsPairRDD = null;
//        userUniqueLocationsRDD = null;
//        userMergedLocationsRDD = null;
//        userGauussianizedMergedVisitsPairRDD = null;
//        PEsOfUsersResultStringPairRDD = null;
//
//
//        //calculate PE_global which is the predictability error of the dataset
//        double PE_global = sum_of_PEs_And_GlobalStringTuple._1 / ppUserFilePaths.size();
//        String globalString = sum_of_PEs_And_GlobalStringTuple._2;
//
//        long endTime = System.nanoTime();
//        System.out.println("||-----------------------------------------------------------------------------------||");
//        System.out.println("|| Distributed PE calculation of " + dataset + " took: "
//                + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");
//
//        return new Tuple2<Double, String>(PE_global, globalString);
//    } // testPEOfADatasetDistributed


    //helper method to print out the Gaussian preprocessig results in a distributed fashion
    public static void printGaussianProcessingResultsDistributed(Dataset dataset, JavaSparkContext sc,
                                                                 List<String> ppUserFilePaths,
                                                                 double thresholdT, double epsilon, int minPts,
                                                                 NextPlaceConfiguration npConf,
                                                                 double trainSplitFraction,
                                                                 double testSplitFraction) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();

        //distance measure for DBSCAN
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };

        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Calculating Gaussian preprocessing results in a distributed fashion for the " + dataset + " .....");
        } // if

        //numSlices to partition the user files
        int numFiles = ppUserFilePaths.size();
        int numSlices = numFiles > 300000 ? numFiles / 1000 : (numFiles > 100000 ? numFiles / 100 :
                (numFiles > 10000 ? numFiles / 10 : (/*numFiles > 6000 ? numFiles / 2 : */numFiles)));


        //accumulator for user files whose PE < 1.0
        LongAccumulator fileCountWhichHasSmaller1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmaller1PE);

        //accumulator for user file whose PE < 0.5
        LongAccumulator fileCountWhichHasSmallerZeroPointFivePE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmallerZeroPointFivePE);

        //accumulator for number of users which has PE > 1.0
        LongAccumulator fileCountWhichHasBigger1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasBigger1PE);

        //accumulator for number of users which has PE < 2.0
        LongAccumulator fileCountWhichHasSmaller2PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmaller2PE);

        //accumulator for number of users which has PE >= 2.0
        LongAccumulator fileCountWhichHasBiggerEqualTo2PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasBiggerEqualTo2PE);

        //accumulator for number of users which are not predictable (has PE == 1.0)
        LongAccumulator fileCountWhichHas1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHas1PE);

        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastHadoopConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        //broadcast configuration npConf, train and test split fractions
        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
        final Broadcast<DistanceMeasure> broadcastDM = sc.broadcast(dm);
        final Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);
        final Broadcast<Double> broadcastThresholdT = sc.broadcast(thresholdT);
        final Broadcast<Double> broadcastEpsilon = sc.broadcast(epsilon);
        final Broadcast<Integer> broadcastMinPts = sc.broadcast(minPts);
        final Broadcast<List<String>> broadcastPPUserFilePaths = sc.broadcast(ppUserFilePaths);


        //max and min cumulative gaussian values of the dataset
        //DoubleAccumulator maxCumulativeWeightedGaussianValueOfTheDataset = new DoubleAccumulator();
        //sc.sc().register(maxCumulativeWeightedGaussianValueOfTheDataset);
        //maxCumulativeWeightedGaussianValueOfTheDataset.add(0);

        //DoubleAccumulator minCumulativeWeightedGaussianValueOfTheDataset = new DoubleAccumulator();
        //sc.sc().register(minCumulativeWeightedGaussianValueOfTheDataset);
        //minCumulativeWeightedGaussianValueOfTheDataset.add(Double.POSITIVE_INFINITY);

        //accumulator for sumOfPEsOfUsers
        DoubleAccumulator sumOfPEsOfUsers = new DoubleAccumulator();
        sc.sc().register(sumOfPEsOfUsers);


        //count of all significant locations of users
        LongAccumulator countOfAllSignificantLocationsOfUsers = new LongAccumulator();
        sc.sc().register(countOfAllSignificantLocationsOfUsers);


        //count of all locations of user; which will be equal to count of all visits of users
        LongAccumulator countOfAllLocationsOfUsers = new LongAccumulator();
        sc.sc().register(countOfAllLocationsOfUsers);


        //total number of visits
        LongAccumulator totalNumberOfSignificantVisits = new LongAccumulator();
        sc.sc().register(totalNumberOfSignificantVisits);

        //total residence seconds after pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);

        //total residence seconds before pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);

        //double average proportion of time spent by each user in significant places
        DoubleAccumulator sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces = new DoubleAccumulator();
        sc.sc().register(sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces);

        //total number of outlier GPS points
        LongAccumulator totalOutlierGPSPoints = new LongAccumulator();
        sc.sc().register(totalOutlierGPSPoints);


        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, numSlices);

        //print train and test split fraction
        System.out.println("\nTrain split vs. test split: "
                + trainSplitFraction * 100 + "%, " + testSplitFraction * 100 + "%");
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism: " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count: " + numSlices);
        System.out.println("Partitions count of the created RDD: " + ppUserFilePathsRDD.partitions().size() + "\n");


        //results rdd will be the string
        JavaRDD<String> resultsRDD = ppUserFilePathsRDD.map(new Function<String, String>() {
            private static final long serialVersionUID = -6255489464624340325L;

            @Override
            public String call(String ppUserFilePath) throws Exception {
                StringBuilder sb = new StringBuilder("");

                //update the fileCount
                //fileCount.add(1);
                //atomicFileCount.add(1);

                String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
                sb.append("---------------------- #").append(broadcastPPUserFilePaths.value().indexOf(ppUserFilePath) + 1).append(": ")
                        .append(keyFileName).append(" ---------------------------------").append("\n");

                //obtain merged visits
                ArrayList<Visit> mergedVisits = correspondingMergedVisits(broadcastDataset.value(), keyFileName, false);

                //total residence seconds of all visits of this user
                long totalResSecondsOfAllVisitsOfThisUser = 0;

                //total residence seconds of significant visits (visits performed to significant locations) of this user
                long totalResSecondsOfSignificantVisitsOfThisUser = 0;


                //update count of all locations of users
                countOfAllLocationsOfUsers.add(mergedVisits.size());


                //obtain \hat_{F_\mu} of each user
                Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                        = correspondingTuple(broadcastDataset.value(), keyFileName, false);

                //min and max weighted cumulative gaussian values for this user
                //double max = 0;
                //double min = Double.POSITIVE_INFINITY;

                //the same location can be considered more than once in the clustering
                ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();


                HashSet<Location> uniqueGPSPoints = new HashSet<Location>();
                for (Visit v : mergedVisits) {
                    Location gpsPoint = v.getLocation();
                    //if(!
                    uniqueGPSPoints.add(gpsPoint); //) System.out.println(v.getLocation());

                    //update total residence seconds of all visits of this user
                    totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();


                    //update total residence of all visits of all users before preprocessing
                    totalResSecondsOfAllVisitsOfUsersBeforePreprocessing.add(v.getTimeSeriesInstant().getResidenceTimeInSeconds());


                    //obtain above threshold gps point
                    double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                    //if(aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;
                    //if(aggregatedWeightedGaussian < min) min = aggregatedWeightedGaussian;


                    //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                    if (aggregatedWeightedGaussian > broadcastThresholdT.value()
                            || Double.compare(aggregatedWeightedGaussian, broadcastThresholdT.value()) == 0) {
                        aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                    } // if
                } // for


                sb.append("Number of merged visits = ").append(mergedVisits.size()).append("\n");
                sb.append("Number of unique locations = ").append(uniqueGPSPoints.size()).append("\n");
                //globalUniqueLocations.addAll(uniqueGPSPoints);
                uniqueGPSPoints.clear();
                uniqueGPSPoints = null;

                //remove location->\hat_F_\mu map of this user
                gpsPointAndWeightedCumulativeGaussianMap.clear();
                gpsPointAndWeightedCumulativeGaussianMap = null;


                sb.append("\nClustering started...").append("\n");
                //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
                //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
                //If these areas are located in different parts of the city,
                //the following code will partition the events in different clusters by looking at each location.
                //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
                // and we start clustering if there are at least three points close to each other.
                //double epsilon = 100;   // default 100, epsilon is in meters for haversine distance
                // = 0.001; // if Kmeans.euclidean, Kmeans.manhattan or Kmeans.cosine distance is used
                //int minPts = 19; // default 3

                sb.append("Total number of above threshold GPS points: ").append(aboveThresholdGPSPointsOfThisUser.size()).append("\n");

                DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(broadcastEpsilon.value(),
                        broadcastMinPts.value(), broadcastDM.value());

                //cluster aboveThresholdGPSPointsOfThisUser
                List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


                //remove the clusters which contain less than minPts + 1 points
                clusters.removeIf(cluster -> cluster.getPoints().size() < broadcastMinPts.value() + 1);

                //clear memory
                int aboveThresholdGPSPointsSize = aboveThresholdGPSPointsOfThisUser.size();
                aboveThresholdGPSPointsOfThisUser.clear();
                aboveThresholdGPSPointsOfThisUser = null;

                //list of all clustered points
                //List<Location> gpsPointsInClusters = new ArrayList<>();
                int totalGPSPointsInClusters = 0;

                //map to hold gps point and its cluster id
                HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

                //lastClusterID variable for each obtained cluster
                int lastClusterID = 1;

                //now for each cluster update list and mp
                for (Cluster<Location> cluster : clusters) {
                    List<Location> gpsPointsInThisCluster = cluster.getPoints();
                    //gpsPointsInClusters.addAll(gpsPointsInThisCluster);
                    totalGPSPointsInClusters += gpsPointsInThisCluster.size();

                    //print size of each cluster
                    sb.append("Cluster_").append(lastClusterID).append(" size: ").append(gpsPointsInThisCluster.size()).append("\n");

                    //now populate location and cluster id map
                    for (Location gpsPoint : gpsPointsInThisCluster) {
                        //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                        gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                    } // for


                    //increment clusterID for next cluster
                    lastClusterID++;
                } // for


                //print number of total locations in all clusters
                sb.append("Number of locations in all clusters: ").append(
                        totalGPSPointsInClusters //gpsPointsInClusters.size()
                ).append("\n");


                //handle that carefully
                //add outlierGPSPoints to be in their single point clusters
                //aboveThresholdGPSPointsOfThisUser.removeAll(gpsPointsInClusters);

                //HashSet<Location> outlierGPSPoints
                //        = aboveThresholdGPSPointsOfThisUser;

                //List<Location> outlierGPSPoints = aboveThresholdGPSPointsOfThisUser;

                sb.append("Number of outlier GPS points: ").append(
                        aboveThresholdGPSPointsSize - totalGPSPointsInClusters ///outlierGPSPoints.size()
                ).append("\n");

                //DO NOT add outlier above threshold gps points as clusters
                //cluster the outliers with minPoints = 0, which will make no outliers,
                //then add resulting cluster to the original clusters
                //DBSCANClusterer<Location> outlierClusterer = new DBSCANClusterer<Location>(epsilon, 0, dm);
                //List<Cluster<Location>> outlierClusters = outlierClusterer.cluster(outlierGPSPoints);
                //for (Cluster<Location> outlierCluster : outlierClusters) {
                //    //update the original clusters
                //    clusters.add(outlierCluster);
                //
                //    //now populate location and cluster id map
                //    for (Location gpsPoint : outlierCluster.getPoints()) {
                //        //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                //        gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                //    } // for
                //
                //    //increment clusterID for next outlier cluster
                //    lastClusterID++;
                //} // for
                totalOutlierGPSPoints.add(aboveThresholdGPSPointsSize - totalGPSPointsInClusters); //(outlierGPSPoints.size());
                //totalOutlierClusters += outlierClusters.size();
                //outlierClusters.clear(); outlierClusters = null;


                //for (Location outlierGPSPoint : outlierGPSPoints) {
                //    Cluster<Location> thisOutlierGPSPointCluster = new Cluster<>();
                //    thisOutlierGPSPointCluster.addPoint(outlierGPSPoint);
                //    //System.out.println("lastClusterID = " + lastClusterID);
                //
                //    //it is possible that no clusters can be generated then, clusterID will be one above
                //    //and we will continue from 1
                //    clusters.add(thisOutlierGPSPointCluster);
                //
                //    //also update gps point and cluster id map
                //    //gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
                //
                //    //increment clusterID for next outlier gps point cluster
                //    lastClusterID++;
                //} // for
                //outlierGPSPoints.clear();
                //outlierGPSPoints = null;
                //aboveThresholdGPSPointsOfThisUser.clear();
                //aboveThresholdGPSPointsOfThisUser = null;
                //gpsPointsInClusters.clear();
                //gpsPointsInClusters = null;


                //index starts from 1
                int clusterIndex = 1; // is the same as lastClusterID, will the value of lastClusterID at the end of below for loop

                //map to hold cluster id and its average location
                HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();

                //for each cluster, compute its average location and store it in a map
                for (Cluster<Location> cluster : clusters) {
                    Location averageLocationForThisCluster = Utils.averageLocation(cluster.getPoints());
                    clusterAverageLocationMap.put(clusterIndex, averageLocationForThisCluster);

                    //increment cluster index
                    clusterIndex++;
                } // for


                //add each outlier GPS point to the nearest cluster
                ////if there are no significant locations, then do not add
                //if (!clusterAverageLocationMap.isEmpty()) {
                //    for (Location outlier : outlierGPSPoints) {
                //        double minDist = Double.POSITIVE_INFINITY;
                //        Integer clusterIDWithClosestCentroid = null;
                //        for (Integer clusterID : clusterAverageLocationMap.keySet()) {
                //            Location thisCentroid = clusterAverageLocationMap.get(clusterID);
                //            double dist = outlier.distanceTo(thisCentroid);
                //            if (dist < minDist) {
                //                minDist = dist;
                //                clusterIDWithClosestCentroid = clusterID;
                //            } // if
                //        } // for
                //
                //        //update gpsPointClusterIDMap -> assign
                //        gpsPointClusterIDMap.put(outlier, clusterIDWithClosestCentroid);
                //    } // for
                //} // if


                //CLUSTERING END...
                sb.append("Clustering ended...\n").append("\n");

                //TESTING
                //writeClusters(ppUserFilePath, mergedVisits, clusters, epsilon, minPts);
                //TESTING


                clusters.clear();
                clusters = null;


                //number of significant locations of this user
                int numSignificantLocationsOfThisUser = clusterAverageLocationMap.keySet().size();


                //list to store significant visits
                ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

                Iterator<Visit> mergedVisitsIterator = mergedVisits.iterator();

                //for each visit, if visit is significant, then reroute it its corresponding significant location
                //for (Visit v : mergedVisits)
                while (mergedVisitsIterator.hasNext()) {
                    Visit v = mergedVisitsIterator.next();

                    Location location = v.getLocation();
                    //check whether this visit's location is within aboveThresholdGPSPoints,
                    //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                    if (gpsPointClusterIDMap.containsKey(location)) {
                        Integer clusterID = gpsPointClusterIDMap.get(location);
                        Location significantLocation = clusterAverageLocationMap.get(clusterID);

                        //reroute visit to its significant location
                        Visit reroutedVisit = new Visit(v.getUserName(), significantLocation, v.getTimeSeriesInstant());
                        gaussianizedVisits.add(reroutedVisit);


                        //update total residence seconds of significant visits
                        totalResSecondsOfSignificantVisitsOfThisUser
                                += reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();


                        //update total residence seconds of all visits of all users after preprocessing
                        totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                                .add(reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds());
                    } // if


                    //remove visit from merged visits list
                    mergedVisitsIterator.remove();
                } // for
                //mergedVisits.clear(); mergedVisits = null;
                gpsPointClusterIDMap.clear();
                gpsPointClusterIDMap = null;


                //update global unique significant locations hash set
                //globalUniqueSignificantLocations.addAll(clusterAverageLocationMap.values());
                clusterAverageLocationMap.clear();
                clusterAverageLocationMap = null;


                //update total proportion of time spent by users by a proportion of this user;
                //at the end of for loop we will divide this number by number of users
                double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
                //if(Double.isNaN(proportionForThisUser)) System.out.println(keyFileName + " is NaN");

                sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces
                        .add(Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser);


                //update max and min cumulative weighted gaussian values of the dataset
                //if(max > maxCumulativeWeightedGaussianValueOfTheDataset.value())
                //{
                //    maxCumulativeWeightedGaussianValueOfTheDataset.reset();
                //    maxCumulativeWeightedGaussianValueOfTheDataset.add(max);
                //} // if
                //if(min < minCumulativeWeightedGaussianValueOfTheDataset.value())
                //{
                //    minCumulativeWeightedGaussianValueOfTheDataset.reset();
                //    minCumulativeWeightedGaussianValueOfTheDataset.add(min);
                //} // if


                //update sum of count of all significant locations of each user
                countOfAllSignificantLocationsOfUsers.add(numSignificantLocationsOfThisUser);

                //total number of significant visits of this user
                totalNumberOfSignificantVisits.add(gaussianizedVisits.size());


                sb.append("Number of significant locations = ").append(numSignificantLocationsOfThisUser).append("\n");
                sb.append("Number of gaussianized visits = ").append(gaussianizedVisits.size()).append("\n");
                //sb.append("Min cumulative weighted gaussian value = ").append(min).append("\n");
                //sb.append("Max cumulative weighted gaussian value = ").append(max).append("\n");


                
                //calculate PE of this user
                double PEOfUser = Utils.peOfPPUserFile(broadcastNpConf.value(),
                        gaussianizedVisits,
                        broadcastTrainSplitFraction.value(),
                        broadcastTestSplitFraction.value());

                if (PEOfUser < 1.0) {
                    fileCountWhichHasSmaller1PE.add(1);
                    //atomicFileCountWhichHasSmaller1PE.add(1);
                }

                if (PEOfUser < 0.5) {
                    fileCountWhichHasSmallerZeroPointFivePE.add(1);
                    //atomicFileCountWhichHasSmallerZeroPointFivePE.add(1);
                }


                if (PEOfUser > 1.0)
                    fileCountWhichHasBigger1PE.add(1);

                if (PEOfUser < 2.0)
                    fileCountWhichHasSmaller2PE.add(1);

                if (PEOfUser > 2.0 || Double.compare(PEOfUser, 2.0) == 0)
                    fileCountWhichHasBiggerEqualTo2PE.add(1);

                //check how many users are not predictable
                if (Double.compare(PEOfUser, 1.0) == 0)
                    fileCountWhichHas1PE.add(1);


                //update the sum
                sumOfPEsOfUsers.add(PEOfUser);


                //pes.add(PEOfUser);
                //double currentPESum = 0;
                //for(double pe : pes)
                //{
                //   currentPESum += pe;
                //}


                sb.append("PE of file with name: ").append(Utils.fileNameFromPath(ppUserFilePath)).append(" => ").append(PEOfUser).append("\n");
                //sb.append("Current average PE of all files: ").append(currentPESum / atomicFileCount.size()).append("\n");
                //sb.append(String.format("%.2f", atomicFileCountWhichHasSmaller1PE.size() * 100 / (1.0 * atomicFileCount.size())))
                //        .append(" % of the current processed files has PE < 1.0").append("\n");
                //sb.append(String.format("%.2f", atomicFileCountWhichHasSmallerZeroPointFivePE.size() * 100 / (1.0 * atomicFileCount.size())))
                //        .append(" % of the current processed files has PE < 0.5").append("\n");

                //sb.append("-----------------------------------------------------------------------\n").append("\n");


                gaussianizedVisits.clear();
                gaussianizedVisits = null;

                String finalResult = sb.toString();

                //System.out.println(finalResult);

                return finalResult;
            } // call
        });


        String finalString = resultsRDD.reduce(new Function2<String, String, String>() {
            private static final long serialVersionUID = -2299986077304016637L;

            @Override
            public String call(String userString1, String userString2) throws Exception {
                return userString1 + userString2;
            } // call
        });


        //print the results for users
        System.out.println(finalString);

        //now calculate average proportion of time spent by each user in significant places
        //by the division of number of users
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces.value() / ppUserFilePaths.size();


        //calculate PE_global which is the predictability error of the dataset
        double PE_global = sumOfPEsOfUsers.value() / ppUserFilePaths.size();


        System.out.println("|| totalResSecondsOfAllVisitsOfUsersBeforePreprocessing => " + totalResSecondsOfAllVisitsOfUsersBeforePreprocessing.value());
        System.out.println("|| totalResSecondsOfAllVisitsOfUsersAfterPreprocessing => " + totalResSecondsOfAllVisitsOfUsersAfterPreprocessing.value());
        System.out.println("|| Total number of locations (visits) of users before pre-processing => " + countOfAllLocationsOfUsers.value());
        //System.out.println("|| Total number of unique locations before pre-processing => " + globalUniqueLocations.size());


        System.out.println("|| Number of users: " + ppUserFilePaths.size());
        System.out.println("|| Total number of (significant) visits: " + totalNumberOfSignificantVisits.value());
        //System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());
        System.out.println("|| Total number of outlier above threshold GPS points: " + totalOutlierGPSPoints.value());


        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                //+ (numberOfAllUniqueLocations - allUniqueLocations.size())
                +countOfAllSignificantLocationsOfUsers.value()
                        / (double) ppUserFilePaths.size()
        ));


        System.out.println("|| Average number of (significant) visits per user: "
                + (int) (totalNumberOfSignificantVisits.value() / (double) ppUserFilePaths.size()));
        System.out.println("|| Average residence time in a place D (seconds): "
                + (int) (
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing.value()
                        //totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                        /// ( 1.0 * globalUniqueSignificantLocations.size() )
                        / (1.0 * countOfAllSignificantLocationsOfUsers.value())
                /// ( 1.0 * globalUniqueLocations.size())
                /// (1.0 * countOfAllLocationsOfUsers)
        ));
        System.out.println("|| Total trace length in days: " + dataset.traceLengthInDays());


        System.out.println("|| Average proportion of time spent by each user in significant places: "
                //+ String.format("%.2f", totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing)
                + String.format("%.2f", avgProportionOfTimeSpentByEachUserInSignificantPlaces)
                + " %");


        System.out.println("|| " + new Date() + " => PE of " + dataset + " is " + PE_global);
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmaller1PE.value() * 100 / (1.0 * ppUserFilePaths.size()))
                + " % of the dataset has PE < 1.0");
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE.value() * 100 / (1.0 * ppUserFilePaths.size()))
                + " % of the dataset has PE < 0.5");
        System.out.println("|| Number of users which has PE > 1.0: " + fileCountWhichHasBigger1PE.value());
        System.out.println("|| Number of users which has PE < 2.0: " + fileCountWhichHasSmaller2PE.value());
        System.out.println("|| Number of users which has PE >= 2.0: " + fileCountWhichHasBiggerEqualTo2PE.value());

        System.out.println("|| Number of users which are not predictable (has PE == 1.0): " + fileCountWhichHas1PE.value());


        //System.out.println("|| Max cumulative weighted gaussian value of the dataset = " + maxCumulativeWeightedGaussianValueOfTheDataset.value());
        //System.out.println("|| Min cumulative weighted gaussian value of the dataset = " + minCumulativeWeightedGaussianValueOfTheDataset.value());

    } // printGaussianProcessingResultsDistributed


    //helper method to calculate the predictability error of a dataset in a distributed fashion
    public static Tuple2<Double, Tuple2<Double, Double>> peOfADatasetDistributed(Dataset dataset, JavaSparkContext sc,
                                                                                 List<String> ppUserFilePaths,
                                                                                 NextPlaceConfiguration npConf,
                                                                                 double trainSplitFraction,
                                                                                 double testSplitFraction) {
        //for finding distributed NextPlace run time
        long startTime = System.nanoTime();

        //print dataset info and s.o.p algorithm run message if not null
        if (dataset != null) {
            dataset.printInfo();
            System.out.println("Distributed PE calculation of the " + dataset + " .....");
        } // if

        //numSlices to partition the user files
        int numFiles = ppUserFilePaths.size();
        int numSlices = numFiles > 300000 ? numFiles / 1000 : (numFiles > 100000 ? numFiles / 100 :
                (numFiles > 10000 ? numFiles / 10 : (/*numFiles > 6000 ? numFiles / 2 : */numFiles)));


        //accumulator for user files whose PE < 1.0
        LongAccumulator fileCountWhichHasSmaller1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmaller1PE);

        //accumulator for user file whose PE < 0.5
        LongAccumulator fileCountWhichHasSmallerZeroPointFivePE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmallerZeroPointFivePE);

        //accumulator for number of users which has PE > 1.0
        LongAccumulator fileCountWhichHasBigger1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasBigger1PE);

        //accumulator for number of users which has PE < 2.0
        LongAccumulator fileCountWhichHasSmaller2PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasSmaller2PE);

        //accumulator for number of users which has PE >= 2.0
        LongAccumulator fileCountWhichHasBiggerEqualTo2PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHasBiggerEqualTo2PE);

        //accumulator for number of users which are not predictable (has PE == 1.0)
        LongAccumulator fileCountWhichHas1PE = new LongAccumulator();
        sc.sc().register(fileCountWhichHas1PE);


        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastHadoopConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        //broadcast configuration npConf, train and test split fractions
        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);


        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, numSlices);

        //print train and test split fraction
        System.out.println("\nTrain split vs. test split: "
                + trainSplitFraction * 100 + "%, " + testSplitFraction * 100 + "%");
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism: " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count: " + numSlices);
        System.out.println("Partitions count of the created RDD: " + ppUserFilePathsRDD.partitions().size() + "\n");


        //------------------------------- OLD -------------------------
        //rdd of PEs of preprocessed user files
        JavaRDD<Double> PEsOfUsersRDD = ppUserFilePathsRDD.map(new Function<String, Double>()
        {
            private static final long serialVersionUID = -4856986363026294231L;

            @Override
            public Double call(String ppUserFilePath) throws Exception {
                //get the file content
                //String ppUserFileContents = Utils.fileContentsFromHDFSFilePath(broadcastHadoopConf.value().value(),
                //                                                               ppUserFilePath, false);
                ////return predictability error of the considered user file
                //double PEOfUser =  Utils.peOfPPUserFile(ppUserFileContents,
                //        broadcastNpConf.value(),
                //        broadcastTrainSplitFraction.value(),
                //        broadcastTestSplitFraction.value());

                InputStream inputStream = Utils.inputStreamFromHDFSFilePath(broadcastHadoopConf.value().value(), ppUserFilePath);

                //return predictability error of the considered user file;
                //inputStream is closed internally
                double PEOfUser = Utils.peOfPPUserFile(inputStream,
                        broadcastNpConf.value(),
                        broadcastTrainSplitFraction.value(),
                        broadcastTestSplitFraction.value());
                //if(PEOfUser > 2.0)
                //    System.out.println("*PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
                //else if(PEOfUser > 1.0)
                //    System.out.println("#PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
                //else if(Double.compare(PEOfUser, 1.0) != 0 && PEOfUser > 0)
                //    System.out.println("$PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
                //else ;
                //    //System.out.println("PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);

                return PEOfUser;
            } // call
        }).cache();     //RDD will be reused, use cache


        //update the accumulators
        PEsOfUsersRDD.foreach((VoidFunction<Double>) PEOfUser -> {
            if (PEOfUser < 1.0)
                fileCountWhichHasSmaller1PE.add(1);

            if (PEOfUser < 0.5)
                fileCountWhichHasSmallerZeroPointFivePE.add(1);

            if (PEOfUser > 1.0)
                fileCountWhichHasBigger1PE.add(1);

            if (PEOfUser < 2.0)
                fileCountWhichHasSmaller2PE.add(1);

            if (PEOfUser > 2.0 || Double.compare(PEOfUser, 2.0) == 0)
                fileCountWhichHasBiggerEqualTo2PE.add(1);

            //check how many users are not predictable
            if (Double.compare(PEOfUser, 1.0) == 0)
                fileCountWhichHas1PE.add(1);
        });


        //now sum up the errors, by reduce step
        double sumOfPEsOfUsers = PEsOfUsersRDD.reduce(new Function2<Double, Double, Double>() {
            private static final long serialVersionUID = -8913427489191550292L;

            @Override
            public Double call(Double PE_1, Double PE_2) throws Exception {
                // OLD
                return PE_1 + PE_2;

                // NEW
                //discard 1.0 PE files from the sum
                //if(Double.compare(PE_1, 1.0) == 0 && Double.compare(PE_2, 1.0) == 0)
                //{
                //    return 0.0;
                //} // if
                //else if(Double.compare(PE_1, 1.0) == 0)
                //    return 0 + PE_2;
                //else if(Double.compare(PE_2, 1.0) == 0)
                //    return PE_1 + 0;
                //else
                //    return PE_1 + PE_2;
            } // call
        });
        //------------------------------- OLD -------------------------


        //------------------------------- NEW -------------------------
//        JavaPairRDD<String, Double> ppUserFilePathPERDD = ppUserFilePathsRDD.mapToPair(new PairFunction<String, String, Double>() {
//            private static final long serialVersionUID = 3336044004052047123L;
//
//            @Override
//            public Tuple2<String, Double> call(String ppUserFilePath) throws Exception {
//                InputStream inputStream = Utils.inputStreamFromHDFSFilePath(broadcastHadoopConf.value().value(), ppUserFilePath);
//
//                //return predictability error of the considered user file;
//                //inputStream is closed internally
//                double PEOfUser = Utils.peOfPPUserFile(inputStream,
//                        broadcastNpConf.value(),
//                        broadcastTrainSplitFraction.value(),
//                        broadcastTestSplitFraction.value());
//
//                return new Tuple2<>(ppUserFilePath, PEOfUser);
//            } // call
//        }).cache();
//
//
//        // call
//        ppUserFilePathPERDD.foreach((VoidFunction<Tuple2<String, Double>>) ppUserFilePathPETuple ->
//        {
//            double PEOfUser = ppUserFilePathPETuple._2;
//
//            if (PEOfUser < 1.0)
//                fileCountWhichHasSmaller1PE.add(1);
//
//            if (PEOfUser < 0.5)
//                fileCountWhichHasSmallerZeroPointFivePE.add(1);
//
//            if (PEOfUser > 1.0)
//                fileCountWhichHasBigger1PE.add(1);
//
//            if (PEOfUser < 2.0)
//                fileCountWhichHasSmaller2PE.add(1);
//
//            if (PEOfUser > 2.0 || Double.compare(PEOfUser, 2.0) == 0)
//                fileCountWhichHasBiggerEqualTo2PE.add(1);
//
//            //check how many users are not predictable
//            if (Double.compare(PEOfUser, 1.0) == 0)
//                fileCountWhichHas1PE.add(1);
//        });
//
//
//        //Map<String, Double> ppUserFilePathPEMap = ppUserFilePathPERDD.collectAsMap();
//        //for(String ppUserFilePath : ppUserFilePaths)
//        //{
//        //    double PEOfUser = ppUserFilePathPEMap.get(ppUserFilePath);
//        //    if(PEOfUser > 2.0)
//        //        System.out.println("*PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
//        //    else if(PEOfUser > 1.0)
//        //        System.out.println("#PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
//        //    else if(Double.compare(PEOfUser, 1.0) != 0 && PEOfUser > 0)
//        //        System.out.println("$PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
//        //    else ;
//        //    //System.out.println("PE of user file: " + fileNameFromPath(ppUserFilePath) + " => " + PEOfUser);
//        //} // for
//
//
//        Map<String, Double> tuples = ppUserFilePathPERDD.collectAsMap();
//        List<String> ppUserFilePathsToBeDiscarded = new ArrayList<>();
//        for (String ppUserFilePath : tuples.keySet()) {
//            // >= 2.0 added
//            if (Double.compare(tuples.get(ppUserFilePath), 1.0) == 0
//                    || Double.compare(tuples.get(ppUserFilePath), 2.0) == 0
//                    || tuples.get(ppUserFilePath) > 2.0
//                //     tuples.get(ppUserFilePath) > 0.5
//            )
//                ppUserFilePathsToBeDiscarded.add(ppUserFilePath.replace("file:/", ""));
//        } // for
//
//        System.out.println("Number of files to be discarded: " + ppUserFilePathsToBeDiscarded.size());
//
//        String origDir = "raw/" + dataset.getName() + "/final_data_100meters";
//        //= "raw/DartmouthWiFi/data_all_loc";
//        //= "raw/IleSansFils/final_data";
//        //move directory
//        String moveDir = "raw/" + dataset.getName() + "/final_data_100meters/discarded";
//        //= "raw/DartmouthWiFi/data_all_loc/non_predictable"; //hasBiggerEqualTo2PE";
//        //= "raw/IleSansFils/final_data/irregular";
//
//        for (String uniqueLocationThresholdSignLocFilePath : ppUserFilePathsToBeDiscarded) {
//            File uniqueLocationThresholdSignLocFile = new File(uniqueLocationThresholdSignLocFilePath);
//            String uniqueLocationThresholdSignLocFileName = uniqueLocationThresholdSignLocFile.getName();
//            uniqueLocationThresholdSignLocFile.renameTo(new File(moveDir
//                    + System.getProperty("file.separator") + uniqueLocationThresholdSignLocFileName));
//        } // for
//
//
//        JavaRDD<Double> PEsOfUsersRDD = ppUserFilePathPERDD.map(new Function<Tuple2<String, Double>, Double>() {
//            private static final long serialVersionUID = 7402624907680718661L;
//
//            @Override
//            public Double call(Tuple2<String, Double> ppUserFilePathPETuple2) throws Exception {
//                return ppUserFilePathPETuple2._2;
//            } // call
//        });
//
//
//        //now sum up the errors, by reduce step
//        double sumOfPEsOfUsers = PEsOfUsersRDD.reduce(new Function2<Double, Double, Double>() {
//            private static final long serialVersionUID = -8913427489191550292L;
//
//            @Override
//            public Double call(Double PE_1, Double PE_2) throws Exception {
//                // OLD
//                return PE_1 + PE_2;
//
//
//                // NEW
//                //discard 1.0 PE files from the sum
//                //if(Double.compare(PE_1, 1.0) == 0 && Double.compare(PE_2, 1.0) == 0)
//                //{
//                //    return 0.0;
//                //} // if
//                //else if(Double.compare(PE_1, 1.0) == 0)
//                //    return 0 + PE_2;
//                //else if(Double.compare(PE_2, 1.0) == 0)
//                //    return PE_1 + 0;
//                //else
//                //    return PE_1 + PE_2;
//            } // call
//        });
        //------------------------------- NEW -------------------------


        //---- NEW ----
//        List<Double> pes = PEsOfUsersRDD.collect();
//        StringBuilder sb = new StringBuilder("");
//        for(double pe : pes)
//        {
//            //if(pe > 2.0); //do nothing
//            if(Double.compare(pe, 1.0) == 0);
//            else
//                sb.append(String.format("%.9f", pe)).append("\n");
//        } // for
//        //pes.forEach(pe -> sb.append(pe /*String.format("%.9f", pe)*/).append("\n"));
//
//
//        String trainTest = "";
//        if(((int) (testSplitFraction * 100)) == 0)
//            trainTest = "_rest_vs_1_";
//        else
//            trainTest = "_" + (100 - ((int) (testSplitFraction * 100))) + "_vs_" + (int) (testSplitFraction * 100) + "_";
//
//        String fileNameAppendix = "pe_" + dataset.toSimpleString()
//                + trainTest + npConf.getDistanceMetric().toSimpleString() + "_m=" + npConf.getEmbeddingParameterM() + "_";
//
//        Utils.writeToLocalFileSystem("raw/" + dataset.getName() + "/final_data/pes/_.csv", fileNameAppendix, "",
//                ".tsv", sb.toString());
        //---- NEW ----


        //unpersist rdds and broadcast conf; make them ready for GC
        //ppUserFilePathPERDD.unpersist();
        ppUserFilePathsRDD.unpersist();
        PEsOfUsersRDD.unpersist();
        broadcastHadoopConf.unpersist();
        broadcastNpConf.unpersist();
        broadcastTrainSplitFraction.unpersist();
        broadcastTrainSplitFraction.unpersist();
        //ppUserFilePathPERDD = null;
        ppUserFilePathsRDD = null;
        PEsOfUsersRDD = null;

        //percentage of users which has PE < 1.0
        double percentageOfFileCountWhichHasSmaller1PE = (fileCountWhichHasSmaller1PE.value() * 100.0) / ppUserFilePaths.size();

        //percentage of users which has PE < 0.5
        double percentageOfFileCountWhichHasSmallerZeroPointFivePE
                = (fileCountWhichHasSmallerZeroPointFivePE.value() * 100.0) / ppUserFilePaths.size();


        //calculate PE_global which is the predictability error of the dataset 
        double PE_global = sumOfPEsOfUsers / ppUserFilePaths.size(); // - fileCountWhichHas1PE.value()); // subtraction discards 1.0 files

        /*
        || Fri Dec 14 20:38:20 UTC 2018 => PE of Dartmouth WiFi dataset is 1.0
        || 0.00 % of the dataset has PE < 1.0
        || 0.00 % of the dataset has PE < 0.5
        || Number of users which has PE > 1.0: 0
        || Number of users which has PE < 2.0: 4847
        || Number of users which has PE >= 2.0: 0
        || Number of users which are not predictable (has PE == 1.0): 4847
        */
        System.out.println("|| " + new Date() + " => PE of " + dataset + " is " + PE_global);
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmaller1PE.value() * 100
                / (1.0 * ppUserFilePaths.size()))
                + " % of the dataset has PE < 1.0");
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE.value() * 100
                / (1.0 * ppUserFilePaths.size()))
                + " % of the dataset has PE < 0.5");
        System.out.println("|| Number of users which has PE > 1.0: " + fileCountWhichHasBigger1PE.value());
        System.out.println("|| Number of users which has PE < 2.0: " + fileCountWhichHasSmaller2PE.value());
        System.out.println("|| Number of users which has PE >= 2.0: " + fileCountWhichHasBiggerEqualTo2PE.value());

        System.out.println("|| Number of users which are not predictable (has PE == 1.0): "
                + fileCountWhichHas1PE.value());


        long endTime = System.nanoTime();
        System.out.println("||-----------------------------------------------------------------------------------||");
        System.out.println("|| Distributed PE calculation of " + dataset + " took: "
                + ((endTime - startTime) / (1000000000 * 60D)) + " minutes");


        //sc.stop();
        //sc.close();
        //sc = null;
        //ppUserFilePathsToBeDiscarded.forEach(System.out::println);


        return new Tuple2<>(PE_global,
                new Tuple2<>(percentageOfFileCountWhichHasSmaller1PE, percentageOfFileCountWhichHasSmallerZeroPointFivePE));
    } // peOfADatasetDistributed


    //helper method to calculate PE of a dataset on files from the local directory path
    public static Tuple2<Double, Tuple2<Double, Double>> peOfADatasetDistributed(Dataset dataset, JavaSparkContext sc,
                                                                                 String hdfsDirPathOfPPUserFiles, boolean recursive,
                                                                                 NextPlaceConfiguration npConf,
                                                                                 double trainSplitFraction,
                                                                                 double testSplitFraction) {
        //list file from hdfs dir
        List<String> ppUserFilePaths
                = Utils.listFilesFromHDFSPath(sc.hadoopConfiguration(), hdfsDirPathOfPPUserFiles, recursive);

        //if directory is empty, print message to system err
        if (ppUserFilePaths.isEmpty()) {
            System.err.println("Resulting files list is empty; either the provided directory is empty"
                    + " or some problem occured when listing files of the directory");

            //since the directory is, there is nothing predictable; return 1.0
            return new Tuple2<>(1.0, new Tuple2<>(0.0, 0.0));
        } else    // paths contain at least one file
        {
            //return the result from overloaded method
            return peOfADatasetDistributed(dataset, sc, ppUserFilePaths,
                    npConf, trainSplitFraction, testSplitFraction);
        } // else

    } // peOfADatasetDistributed


    //method to find the predictability error of all visit histories one of user
    //global predictability error can be calculated by summing up
    //the predictability error of each user and then dividing number of users
    public static double peOfPPUserFile(NextPlaceConfiguration npConf, ArrayList<Visit> mergedVisits,
                                        double trainSplitFraction, double testSplitFraction) {
        //to calculate the predictability error of all visit histories of a user
        //we need to calculate the predictability error of one visit history
        //and to do that, we need calculate "mean quadratic prediction error" and variance of time series
        //of chosen visit history;


        /* // LESS RELIABLE METHOD
        //------------------------------------------------------------------------------------------
        //we take frequency threshold as minimum, as compared to highestVisitFrequency / 2;
        //because we would like to have all merged visits to be considered in PE calculation;
        int frequencyThreshold = 1;

        //create array list of visit histories to different unique locations
        //HashSet is also possible, but changes visit history orders
        //based on generated hash of the object
        ArrayList<VisitHistory> visitHistories = createVisitHistories(mergedVisits,
                frequencyThreshold);
        //------------------------------------------------------------------------------------------
        */  // LESS RELIABLE METHOD


        //---------------------  MORE RELIABLE METHOD ---------------------------------------
        int highestFrequency = highestVisitFrequencyOfUser(mergedVisits);
        //maxK related to the highest visit frequency cannot be equal to or smaller than 0
        //otherwise user file is not predictable, because future visits
        //cannot be predicted from file's highest frequent visit (location, which will correspond to visit history)
        int maxKOfHighestFrequentVisitHistory = maxValueOfFutureStepK(npConf, highestFrequency);
        if (maxKOfHighestFrequentVisitHistory <= 0) {
            //if it is, then the user's file is not predictable, return 1.0 (which means time series are not predictable)
            //since highest frequent visit (history) is not predictable
            //then all other visits (histories) are not also predictable; which will result predictability error of 1.0;
            //we just omit the calculation here
            System.err.println("User file is not predictable (Highest Frequent Visit History is not predictable); maxKOfHighestFrequentVisitHistory = "
                    + maxKOfHighestFrequentVisitHistory + "; returning 1.0 for PE");
            return 1.0;
        } // if


        //we will do train/test split of 95%/5% or 90%/10%
        //Here maxKOfHighestFrequentVisitHistoryOfUser is at least 1, so
        //create array list of visit histories to different unique locations;
        //this createVisitHistories() method is more reliable, since
        //visit history of visits whose maxK <= 0 will not be created;
        //only predictable visit histories will be created (whose first half is predictable for the given configuration)
        Collection<VisitHistory> visitHistories =
                createVisitHistories(mergedVisits, npConf, trainSplitFraction, testSplitFraction);

        //createVisitHistories(mergedVisits, 1);

        //createVisitHistories(mergedVisits,
        //        new NextPlaceConfiguration(3,
        //                npConf.getDelayParameterV(),
        //                npConf.getTimeTInDaySeconds(),
        //                npConf.getDeltaTSeconds(), npConf.getDistanceMetric()), true);

//        System.out.println("Number of visit histories: " + visitHistories.size());
//        int count = 1;
//        for(VisitHistory vh : visitHistories)
//        {
//            System.out.println("#" + count + " vh size = " + vh.getFrequency());
//            count ++;
//        } // for
        //---------------------  MORE RELIABLE METHOD ---------------------------------------


        //sum of all predictability errors of all visit histories that user has at this point
        double sumOfPEsOfAllVisitHistories = 0;


        //create algorithm instance
        NextPlace algorithm = new NextPlace(npConf);


        int minNumNeighborsToStart = 1;
        int maxNumNeighborsToFinish = 200;


        //we have generated predictable visit histories with sufficient length
        //but, still it is possible that some of visit histories still won't have
        //embedding vectors in their epsilon-neighborhood;
        //therefore, we define the variable for considered visit histories for division
        int numberOfConsideredVisitHistories = 0;


        //loop through all visit histories by calculating their PE
        for (VisitHistory visitHistory : visitHistories) {
            //METHOD 1: choose different optimal number of neighbors for each visit history
//            Tuple2<Double, Integer> bestPEBestNumNeighborsTuple
//                    = bestPEBestNumNeighbors(visitHistory, algorithm,
//                                            minNumNeighborsToStart, maxNumNeighborsToFinish,
//                                            trainSplitFraction, testSplitFraction);
//            double bestPE = bestPEBestNumNeighborsTuple._1;
//            sumOfPEsOfAllVisitHistories += bestPE;


            //METHOD 2: choose different list of optimal number of neighbors for each visit history; for each deltaL
//            ArrayList<TimeSeriesInstant> originalTimeSeries = visitHistory.getTimeSeries();
//
//            //length of the time series
//            int originalTimeSeriesLength = originalTimeSeries.size();
//
//            if(originalTimeSeriesLength < 3
//                    || originalTimeSeriesLength - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                    * algorithm.getNpConf().getDelayParameterV() < 2) //maxK < 1 means not predictable
//            {
//                //System.err.println("The original time series (length = " + originalTimeSeriesLength + ") is not predictable, returning 1.0 for PE");
//                sumOfPEsOfAllVisitHistories += 1.0;
//                continue; // visit history is not predictable, therefore continue to the next visit history
//            } // if
//
//            //we make train test split of 95% and 5% or 90% and 10%, then check whether first half is predictable
//            //calculate the length of first and second part
//            int lengthOfSecondPart = (int) (originalTimeSeriesLength * testSplitFraction);
//            if(lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
//            int lengthOfFirstPart = originalTimeSeriesLength - lengthOfSecondPart;
//
//            //if by integer division accidentally first part and second part have same size, make second part one smaller
//            //and first part one bigger
//            if(lengthOfFirstPart == lengthOfSecondPart)
//            {
//                lengthOfSecondPart -= 1;
//                lengthOfFirstPart += 1;
//            } // if
//
//
//            if(lengthOfFirstPart
//                    - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                    * algorithm.getNpConf().getDelayParameterV() < 2) // => maxKForFirstPart < 1; where maxKForFirstPart is max #neighbors of B_N
//            {
//                //System.err.println("First half of the time series is not predictable, returning 1.0 for PE");
//                sumOfPEsOfAllVisitHistories += 1.0;
//                continue; // visit history is not predictable, therefore continue to the next visit history
//            } // if
//
//
//            Tuple2<VisitHistory, VisitHistory> firstSecondTuple2
//                    = visitHistory.divide(algorithm.getNpConf(), trainSplitFraction, testSplitFraction);
//            VisitHistory firstPartVisitHistory = firstSecondTuple2._1;
//            VisitHistory secondPartVisitHistory = firstSecondTuple2._2;
//
//
//            //create an embedding space for the time series of visit history
//            //create distance-index pairs for every beta_n with beta_N
//            Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
//                    = algorithm.createEmbeddingSpace(firstPartVisitHistory.getTimeSeries());
//            LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
//            int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
//            //int NIndex = alphaGlobal.lastKey();
//            EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);
//
//
//
//            //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
//            //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
//            //where Double.compareTo() method uses long bits of a Double number;
//            //we will sort values of this map which are indices in descending order to collect recent indexed neighbors first
//            //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]}
//
//            SetMultimap<Double, Integer> distanceIndexPairs = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
//            //ListMultimap<Double, Integer> distIndexPairs = MultimapBuilder.treeKeys().arrayListValues().build();
//
//            //iterate over all B_n in alphaGlobal,
//            //alphaGlobal contains keys sorted in ascending order
//            alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
//            for(Integer nIndex : alphaGlobal.keySet())
//            {
//                double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
//                distanceIndexPairs.put(distance, nIndex);
//            } // for
//            alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place
//
//            //NO NOTICEABLE PERFORMANCE IMPROVEMENT
//            //linked hash multi map is more efficient in terms of get, remove operations
//            //it will preserve ordering in passed tree multi map
//            //distanceIndexPairs = LinkedHashMultimap.create(distanceIndexPairs);
//
//
//            //it is possible that secondPartVisitHistory.getFrequency() can be bigger than
//            //firstPartVisitHistory.maxPossibleK(npConf), in this case we choose smaller one
//            int numFutureVisitsToPredict
//                    = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) > secondPartVisitHistory.getFrequency()
//                    ? secondPartVisitHistory.getFrequency() : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//
//            //int bestNumNeighbors = 0;
//            int maxNumNeighbors = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) >
//                    //200 ? 200
//                    maxNumNeighborsToFinish ? maxNumNeighborsToFinish
//                    : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//            //it is possible that numNeighbors dor delta_L = 3 can be different from delta_L = 1
//            //in this case, starting from 1 to numFutureVisitsToPredict list of best numNeighbors should be generated;
//            //for each deltaL starting from 1 to numFutureVisitsToPredict
//            //find the best numNeighbors and put it in an array, then use it inside predictAVisitKStepsAhead() method;
//            //assume that embedding space is constructed with embedding vectors B1, B2, B3, B4, B5
//            //when deltaL = 1 => B1, B2, B3, B4 are considered in neighborhood search, suppose 2 is the opt. num neighbors;
//            //now when deltaL = 2 => B1, B2, B3 are considered in NS, now suppose 1 is the opt. num neighbors
//            //double[] bestPEForDeltaL = new double[numFutureVisitsToPredict];
//            int[] bestNumNeighborsForDeltaLs = new int[numFutureVisitsToPredict];
//            for(int deltaL = 1; deltaL <= numFutureVisitsToPredict; deltaL ++)
//            {
//                double bestPEForThisDeltaL = Double.POSITIVE_INFINITY;
//                int bestNumNeighborsForThisDeltaL = 0;
//
//                for(int numNeighbors = minNumNeighborsToStart; /*1;*/ numNeighbors <= /*distanceIndexPairs.values().size() / 2;*/
//                        maxNumNeighbors; /*firstPartVisitHistory.maxPossibleK(npConf);*/
//                    numNeighbors ++)
//                {
//                    //predict a visit deltaL steps ahead
//                    Visit predictedVisitAtStepDeltaL = algorithm.predictAVisitKStepsAhead(deltaL,
//                            firstPartVisitHistory,
//                            numNeighbors,
//                            alphaGlobal, indexOfBeta_N,
//                            distanceIndexPairs);
//
//                    //calculate MQPE and variance
//                    double MQPE = algorithm.MQPE(secondPartVisitHistory.getTimeSeries().get(deltaL - 1),
//                                                    predictedVisitAtStepDeltaL.getTimeSeriesInstant());
//                    double variance = Utils.populationVarianceOfTimeSeries(visitHistory.getTimeSeries());
//
//
//                    //System.out.println("MQPE: " + MQPE + "\nVariance: " + variance);
//                    if(Double.isNaN(MQPE / variance)) throw new RuntimeException("variance of the time series is zero");
//
//                    //predictability error which is MQPE / variance
//                    double predictabilityErrorOfVisitHistoryForThisDeltaL = MQPE / variance;
//
//
//                    if(predictabilityErrorOfVisitHistoryForThisDeltaL < bestPEForThisDeltaL)
//                    {
//                        bestPEForThisDeltaL = predictabilityErrorOfVisitHistoryForThisDeltaL;
//                        bestNumNeighborsForThisDeltaL = numNeighbors;
//                    } // if
//                } // for
//
//                //update arrays
//                //bestPEForDeltaL[deltaL - 1] = bestPEForThisDeltaL;
//                bestNumNeighborsForDeltaLs[deltaL - 1] = bestNumNeighborsForThisDeltaL;
//            } // for
//
//
//            //now call peOfVisitHistory with best num neighbors list
//            sumOfPEsOfAllVisitHistories += algorithm.peOfVisitHistory(visitHistory,
//                                            firstPartVisitHistory, secondPartVisitHistory,
//                                            numFutureVisitsToPredict,
//                                            alphaGlobal,
//                                            indexOfBeta_N,
//                                            distanceIndexPairs,
//                                            //HashMap<Integer, Double> indexDistancePairs)
//                                            bestNumNeighborsForDeltaLs);


            // OLD
            //get predictability error
            //if the visit history does not have any embedding vectors in its epsilon-neighborhood
            //they will be discarded, their PE will be -1
            double predictabilityErrorOfVisitHistory
                    = algorithm.peOfVisitHistory(visitHistory, trainSplitFraction, testSplitFraction);

            /*
            System.out.println("\n======= Visit history #" + (index + 1)
                    + "; Frequency: " + visitHistory.getFrequency()
                    + " =======");

            System.out.println("Predictability error: "
                    + predictabilityErrorOfVisitHistory);
            */

            //sumOfPEsOfAllVisitHistories += predictabilityErrorOfVisitHistory;


            //NEW
            if (predictabilityErrorOfVisitHistory != -1) {
                sumOfPEsOfAllVisitHistories += predictabilityErrorOfVisitHistory;
                numberOfConsideredVisitHistories++;
            } // if
            // OLD
        } // for


        //PE_i refers to predictability error of user i's all visit histories
        //which is the average over visitHistories.size()
        double PE_i;


        //it is possible that sometimes predictable visit histories
        //are not generated for the user, in this case we will have
        //visitHistories.size() == 0 and in turn sumOfPEsOfAllVisitHistories == 0.0
        //which results 0 / 0 => NaN, therefore if visitHistories.size() == 0
        //we will return 1.0 meaning that user's file is not predictable
        //(because there are not predictable visit histories that can be generated from a user's file)
        if (visitHistories.size() == 0) {
            System.err.println("Predictable visit histories are not generated for the user which means user's file is not predictable; returning 1.0 for PE");
            PE_i = 1.0;
        } // if
        // NEW else if
        else if (numberOfConsideredVisitHistories == 0) {
            //System.err.println("All visit histories do not any e.v. in their epsilon neighborhood; returning 1.0 for PE");
            PE_i = 1.0;
        } else PE_i = sumOfPEsOfAllVisitHistories / numberOfConsideredVisitHistories; //visitHistories.size();

        //System.out.println("sumOfPEsOfAllVisitHistories: " + sumOfPEsOfAllVisitHistories + "\nvisitHistories.size(): " + visitHistories.size());
        //if(Double.isNaN(sumOfPEsOfAllVisitHistories / visitHistories.size())) System.exit(0);

        //return the result
        return PE_i;
    } // peOfPPUserFile


    //method to find the predictability error of all visit histories one of user
    //global predictability error can be calculated by summing up
    //the predictability error of each user and then dividing number of users
    //one should provide input stream of the user file to it; either network file or local file input stream
    public static double peOfPPUserFile(InputStream ppUserFileInputStream,
                                        NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction) {
        //get merged visits
        ArrayList<Visit> mergedVisits = visitsFromPPUserFileInputStream(ppUserFileInputStream);

        //above method will return empty array list if file reading or any other problem occurs;
        //ir will also return empty list if pre-processed file is empty;
        //if this happens, return from method with 1.0 meaning that user's file is not predictable
        if (mergedVisits.isEmpty()) {
            System.err.println("#Merged visits is 0 for the user; returning 1.0 for PE");
            return 1.0;
        } // if


        //call overloaded method; that method has one less step than this method
        //it is calculating based on mergedVisits array list
        return peOfPPUserFile(npConf, mergedVisits, trainSplitFraction, testSplitFraction);
    } // peOfPPUserFile


    //PE of the pp user file from its file contents
    public static double peOfPPUserFile(String ppUserFileContents,
                                        NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction) {
        //get merged visits
        ArrayList<Visit> mergedVisits = visitsFromPPUserFileContents(ppUserFileContents);

        //above method will return empty array list if file reading or any other problem occurs;
        //ir will also return empty list if pre-processed file is empty;
        //if this happens, return from method with 1.0 meaning that user's file is not predictable
        if (mergedVisits.isEmpty()) {
            System.err.println("#Merged visits is 0 for the user; returning 1.0 for PE");
            return 1.0;
        } // if


        //call overloaded method; that method has one less step than this method
        //it is calculating based on mergedVisits array list
        return peOfPPUserFile(npConf, mergedVisits, trainSplitFraction, testSplitFraction);
    } // peOfPPUserFile


//    //helper method to calculate best PE and best num neighbors tuple
//    public static Tuple2<Double, Integer> bestPEBestNumNeighbors(VisitHistory visitHistory, NextPlace algorithm,
//                                                                 int minNumNeighborsToStart, int maxNumNeighborsToFinish,
//                                                                 double trainSplitFraction, double testSplitFraction
//    ) {
//        double bestPE = Double.POSITIVE_INFINITY;
//
//        ArrayList<TimeSeriesInstant> originalTimeSeries = visitHistory.getTimeSeries();
//
//        //length of the time series
//        int originalTimeSeriesLength = originalTimeSeries.size();
//
//        if (originalTimeSeriesLength < 3
//                || originalTimeSeriesLength - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                * algorithm.getNpConf().getDelayParameterV() < 2) //maxK < 1 means not predictable
//        {
//            //System.err.println("The original time series (length = " + originalTimeSeriesLength + ") is not predictable, returning 1.0 for PE");
//            return new Tuple2<Double, Integer>(1.0, -1); // return 1.0 as PE and -1 as num neighbors
//        } // if
//
//        //we make train test split of 95% and 5% or 90% and 10%, then check whether first half is predictable
//        //calculate the length of first and second part
//        int lengthOfSecondPart = (int) (originalTimeSeriesLength * testSplitFraction);
//        if (lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
//        int lengthOfFirstPart = originalTimeSeriesLength - lengthOfSecondPart;
//
//        //if by integer division accidentally first part and second part have same size, make second part one smaller
//        //and first part one bigger
//        if (lengthOfFirstPart == lengthOfSecondPart) {
//            lengthOfSecondPart -= 1;
//            lengthOfFirstPart += 1;
//        } // if
//
//
//        if (lengthOfFirstPart
//                - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                * algorithm.getNpConf().getDelayParameterV() < 2) // => maxKForFirstPart < 1; where maxKForFirstPart is max #neighbors of B_N
//        {
//            //System.err.println("First half of the time series is not predictable, returning 1.0 for PE");
//            return new Tuple2<Double, Integer>(1.0, -1); // return 1.0 as PE and -1 as num neighbors
//        } // if
//
//
//        Tuple2<VisitHistory, VisitHistory> firstSecondTuple2
//                = visitHistory.divide(algorithm.getNpConf(), trainSplitFraction, testSplitFraction);
//        VisitHistory firstPartVisitHistory = firstSecondTuple2._1;
//        VisitHistory secondPartVisitHistory = firstSecondTuple2._2;
//
//
//        //create an embedding space for the time series of visit history
//        //create distance-index pairs for every beta_n with beta_N
//        Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
//                = algorithm.createEmbeddingSpace(firstPartVisitHistory.getTimeSeries());
//        LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
//        int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
//        //int NIndex = alphaGlobal.lastKey();
//        EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);
//
//
//        //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
//        //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
//        //where Double.compareTo() method uses long bits of a Double number;
//        //we will sort values of this map which are indices in descending order to collect recent indexed neighbors first
//        //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]}
//
//        SetMultimap<Double, Integer> distanceIndexPairs = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
//        //ListMultimap<Double, Integer> distIndexPairs = MultimapBuilder.treeKeys().arrayListValues().build();
//
//        //iterate over all B_n in alphaGlobal,
//        //alphaGlobal contains keys sorted in ascending order
//        alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
//        for (Integer nIndex : alphaGlobal.keySet()) {
//            double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
//            distanceIndexPairs.put(distance, nIndex);
//        } // for
//        alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place
//
//        //NO NOTICEABLE PERFORMANCE IMPROVEMENT
//        //linked hash multi map is more efficient in terms of get, remove operations
//        //it will preserve ordering in passed tree multi map
//        //distanceIndexPairs = LinkedHashMultimap.create(distanceIndexPairs);
//
//
//        //it is possible that secondPartVisitHistory.getFrequency() can be bigger than
//        //firstPartVisitHistory.maxPossibleK(npConf), in this case we choose smaller one
//        int numFutureVisitsToPredict
//                = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) > secondPartVisitHistory.getFrequency()
//                ? secondPartVisitHistory.getFrequency() : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//        int bestNumNeighbors = 0;
//        int maxNumNeighbors = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) >
//                //200 ? 200
//                maxNumNeighborsToFinish ? maxNumNeighborsToFinish
//                : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//        //test starts number of neighbors from minNumNeighborsToStart to maxNumNeighbors
//        for (int numNeighbors = minNumNeighborsToStart; /*1;*/ numNeighbors <= /*distanceIndexPairs.values().size() / 2;*/
//                maxNumNeighbors; /*firstPartVisitHistory.maxPossibleK(npConf);*/
//             numNeighbors++) {
//            //the same numNeighbors are used for each deltaL = 1, 2, 3
//            double predictabilityErrorOfVisitHistory
//                    = algorithm.peOfVisitHistory(visitHistory, firstPartVisitHistory, secondPartVisitHistory,
//                    numFutureVisitsToPredict,
//                    alphaGlobal, indexOfBeta_N, distanceIndexPairs, numNeighbors);
//
//            if (predictabilityErrorOfVisitHistory < bestPE) {
//                bestPE = predictabilityErrorOfVisitHistory;
//                bestNumNeighbors = numNeighbors;
//            } // if
//        } // for
//
//
//        return new Tuple2<>(bestPE, bestNumNeighbors);
//    } // bestPEBestNumNeighbors


//    //calculates the best number of neighbors for each deltaL starting from 1 up to numFutureVisitsToPredict
//    public static int[] bestNumNeighborsForEachDeltaL(VisitHistory visitHistory, NextPlace algorithm,
//                                                      int minNumNeighborsToStart, int maxNumNeighborsToFinish,
//                                                      double trainSplitFraction, double testSplitFraction) {
//        //double bestPE = Double.POSITIVE_INFINITY;
//
//        ArrayList<TimeSeriesInstant> originalTimeSeries = visitHistory.getTimeSeries();
//
//        //length of the time series
//        int originalTimeSeriesLength = originalTimeSeries.size();
//
//        if (originalTimeSeriesLength < 3
//                || originalTimeSeriesLength - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                * algorithm.getNpConf().getDelayParameterV() < 2) //maxK < 1 means not predictable
//        {
//            //System.err.println("The original time series (length = " + originalTimeSeriesLength + ") is not predictable, returning 1.0 for PE");
//            return new int[]{-1}; // return -1 as num neighbors
//        } // if
//
//        //we make train test split of 95% and 5% or 90% and 10%, then check whether first half is predictable
//        //calculate the length of first and second part
//        int lengthOfSecondPart = (int) (originalTimeSeriesLength * testSplitFraction);
//        if (lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
//        int lengthOfFirstPart = originalTimeSeriesLength - lengthOfSecondPart;
//
//        //if by integer division accidentally first part and second part have same size, make second part one smaller
//        //and first part one bigger
//        if (lengthOfFirstPart == lengthOfSecondPart) {
//            lengthOfSecondPart -= 1;
//            lengthOfFirstPart += 1;
//        } // if
//
//
//        if (lengthOfFirstPart
//                - (algorithm.getNpConf().getEmbeddingParameterM() - 1)
//                * algorithm.getNpConf().getDelayParameterV() < 2) // => maxKForFirstPart < 1; where maxKForFirstPart is max #neighbors of B_N
//        {
//            //System.err.println("First half of the time series is not predictable, returning 1.0 for PE");
//            return new int[]{-1}; // return -1 as num neighbors
//        } // if
//
//
//        Tuple2<VisitHistory, VisitHistory> firstSecondTuple2
//                = visitHistory.divide(algorithm.getNpConf(), trainSplitFraction, testSplitFraction);
//        VisitHistory firstPartVisitHistory = firstSecondTuple2._1;
//        VisitHistory secondPartVisitHistory = firstSecondTuple2._2;
//
//
//        //create an embedding space for the time series of visit history
//        //create distance-index pairs for every beta_n with beta_N
//        Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
//                = algorithm.createEmbeddingSpace(firstPartVisitHistory.getTimeSeries());
//        LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
//        int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
//        //int NIndex = alphaGlobal.lastKey();
//        EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);
//
//
//        //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
//        //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
//        //where Double.compareTo() method uses long bits of a Double number;
//        //we will sort values of this map which are indices in descending order to collect recent indexed neighbors first
//        //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]}
//
//        SetMultimap<Double, Integer> distanceIndexPairs = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
//        //ListMultimap<Double, Integer> distIndexPairs = MultimapBuilder.treeKeys().arrayListValues().build();
//
//        //iterate over all B_n in alphaGlobal,
//        //alphaGlobal contains keys sorted in ascending order
//        alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
//        for (Integer nIndex : alphaGlobal.keySet()) {
//            double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
//            distanceIndexPairs.put(distance, nIndex);
//        } // for
//        alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place
//
//        //NO NOTICEABLE PERFORMANCE IMPROVEMENT
//        //linked hash multi map is more efficient in terms of get, remove operations
//        //it will preserve ordering in passed tree multi map
//        //distanceIndexPairs = LinkedHashMultimap.create(distanceIndexPairs);
//
//
//        //it is possible that secondPartVisitHistory.getFrequency() can be bigger than
//        //firstPartVisitHistory.maxPossibleK(npConf), in this case we choose smaller one
//        int numFutureVisitsToPredict
//                = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) > secondPartVisitHistory.getFrequency()
//                ? secondPartVisitHistory.getFrequency() : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//        //int bestNumNeighbors = 0;
//        int maxNumNeighbors = firstPartVisitHistory.maxPossibleK(algorithm.getNpConf()) >
//                //200 ? 200
//                maxNumNeighborsToFinish ? maxNumNeighborsToFinish
//                : firstPartVisitHistory.maxPossibleK(algorithm.getNpConf());
//
//
//        //it is possible that numNeighbors dor delta_L = 3 can be different from delta_L = 1
//        //in this case, starting from 1 to numFutureVisitsToPredict list of best numNeighbors should be generated;
//        //for each deltaL starting from 1 to numFutureVisitsToPredict
//        //find the best numNeighbors and put it in an array, then use it inside predictAVisitKStepsAhead() method;
//        //assume that embedding space is constructed with embedding vectors B1, B2, B3, B4, B5
//        //when deltaL = 1 => B1, B2, B3, B4 are considered in neighborhood search, suppose 2 is the opt. num neighbors;
//        //now when deltaL = 2 => B1, B2, B3 are considered in NS, now suppose 1 is the opt. num neighbors
//        //double[] bestPEForDeltaL = new double[numFutureVisitsToPredict];
//        int[] bestNumNeighborsForDeltaLs = new int[numFutureVisitsToPredict];
//        for (int deltaL = 1; deltaL <= numFutureVisitsToPredict; deltaL++) {
//            double bestPEForThisDeltaL = Double.POSITIVE_INFINITY;
//            int bestNumNeighborsForThisDeltaL = 0;
//
//            for (int numNeighbors = minNumNeighborsToStart; /*1;*/ numNeighbors <= /*distanceIndexPairs.values().size() / 2;*/
//                    maxNumNeighbors; /*firstPartVisitHistory.maxPossibleK(npConf);*/
//                 numNeighbors++) {
//                //predict a visit deltaL steps ahead
//                Visit predictedVisitAtStepDeltaL = algorithm.predictAVisitKStepsAhead(deltaL,
//                        firstPartVisitHistory,
//                        numNeighbors,
//                        alphaGlobal, indexOfBeta_N,
//                        distanceIndexPairs);
//
//                //generate time series for predicted visit
//                ArrayList<TimeSeriesInstant> predictedSecondPart = new ArrayList<TimeSeriesInstant>();
//                predictedSecondPart.add(predictedVisitAtStepDeltaL.getTimeSeriesInstant());
//
//                //calculate MQPE and variance
//                double MQPE = algorithm.MQPE(secondPartVisitHistory.getTimeSeries(), predictedSecondPart);
//                double variance = Utils.populationVarianceOfTimeSeries(visitHistory.getTimeSeries());
//
//
//                //System.out.println("MQPE: " + MQPE + "\nVariance: " + variance);
//                if (Double.isNaN(MQPE / variance)) throw new RuntimeException("variance of the time series is zero");
//
//                //predictability error which is MQPE / variance
//                double predictabilityErrorOfVisitHistoryForThisDeltaL = MQPE / variance;
//
//
//                if (predictabilityErrorOfVisitHistoryForThisDeltaL < bestPEForThisDeltaL) {
//                    bestPEForThisDeltaL = predictabilityErrorOfVisitHistoryForThisDeltaL;
//                    bestNumNeighborsForThisDeltaL = numNeighbors;
//                } // if
//            } // for
//
//            //update arrays
//            //bestPEForDeltaL[deltaL - 1] = bestPEForThisDeltaL;
//            bestNumNeighborsForDeltaLs[deltaL - 1] = bestNumNeighborsForThisDeltaL;
//        } // for
//
//        return bestNumNeighborsForDeltaLs;
//    } // bestNumNeighborsForEachDeltaL


    private static void ensureCorrectnessOfTDeltaTAndErrorMargin(long timeTInDaySeconds,
                                                                 long deltaTSeconds,
                                                                 long errorMarginThetaInSeconds) {
        if (timeTInDaySeconds < 0)
            throw new RuntimeException("Time T cannot be negative");
        if (deltaTSeconds < 0)
            throw new RuntimeException("Delta T cannot be negative");
        if (errorMarginThetaInSeconds < 0)
            throw new RuntimeException("Error margin Theta cannot be negative");
    } // ensureCorrectnessOfTDeltaTAndErrorMargin


    //helper method to find a prediction precision of a user in a different way
    public static LinkedHashMap<Long, Double> predictionPrecisionOfAUserAfterDeltaTInterval(NextPlaceConfiguration npConf,
                                                                                            ArrayList<Visit> visits,
                                                                                            double trainSplitFraction,
                                                                                            double testSplitFraction,
                                                                                            long[] deltaSecs,
                                                                                            int numPredictionsToPerform,
                                                                                            long seed,
                                                                                            long errorMarginThetaInSeconds)
    {
        if (errorMarginThetaInSeconds < 0)
            throw new RuntimeException("Error margin Theta cannot be negative");


        //hash map to hold delta sec and prediction precision
        LinkedHashMap<Long, Double> deltaSecPredictionPrecisionMap = new LinkedHashMap<>();

        //create two dividable visit histories
        //---------------------  MORE RELIABLE METHOD ---------------------------------------
        int highestFrequency = highestVisitFrequencyOfUser(visits);
        //maxK related to the highest visit frequency cannot be equal to or smaller than 0
        //otherwise user file is not predictable, because future visits
        //cannot be predicted from file's highest frequent visit (location, which will correspond to visit history)
        int maxKOfHighestFrequentVisitHistory = maxValueOfFutureStepK(npConf, highestFrequency);
        if (maxKOfHighestFrequentVisitHistory <= 0) {
            //if it is, then the user's file is not predictable, return 1.0 (which means time series are not predictable)
            //since highest frequent visit (history) is not predictable
            //then all other visits (histories) are not also predictable; which will result predictability error of 1.0;
            //we just omit the calculation here
            System.err.println("Highest Frequent Visit History is not predictable; maxKOfHighestFrequentVisitHistory = "
                    + maxKOfHighestFrequentVisitHistory + "; returning null for Prediction Precision at each T+delta_T");
            return null;
        } // if


        //int kElementsForTest = 8;
        //Here maxKOfHighestFrequentVisitHistoryOfUser is at least 1, so
        //create array list of visit histories to different unique locations;
        //this createVisitHistories() method is more reliable, since
        //visit history of visits whose maxK <= 0 will not be created;
        //only predictable visit histories will be created
        Collection<VisitHistory> visitHistories
                //= createVisitHistories(visits, npConf, true);
                = createVisitHistories(visits, npConf, trainSplitFraction, testSplitFraction);
        //= createVisitHistories(visits, npConf, kElementsForTest); //  at max 8 elements can be chosen for test
        //---------------------  MORE RELIABLE METHOD ---------------------------------------

        NextPlace algorithm = new NextPlace(npConf);

        //visit histories which contain the first half of these visit histories
        ArrayList<VisitHistory> firstPartVisitHistories = new ArrayList<VisitHistory>();

        //visit histories which contain the second half of these visit histories
        ArrayList<VisitHistory> secondPartVisitHistories = new ArrayList<VisitHistory>();

        //loop through all visit histories to generate first half and second half visit histories
        for (VisitHistory visitHistory : visitHistories) {
            //get the first part
            Tuple2<VisitHistory, VisitHistory> firstSecondTuple
                    = visitHistory
                    .divide(npConf, trainSplitFraction, testSplitFraction);
            //.divide(npConf, kElementsForTest); //  at max 8 elements can be chosen for test

            VisitHistory firstHalfVisitHistory = firstSecondTuple._1;
            firstPartVisitHistories.add(firstHalfVisitHistory);


            //now get the second part
            VisitHistory secondHalfVisitHistory = firstSecondTuple._2;
            secondPartVisitHistories.add(secondHalfVisitHistory);
        } // for


//        // NEW
//        //get trainSplitFraction of visits as a training
//        ArrayList<Visit> firstPartVisits = (ArrayList<Visit>) Utils.head( visits, (int) (visits.size() * trainSplitFraction) );
//        ArrayList<VisitHistory> firstPartVisitHistories = new ArrayList<>(
//                Utils.createVisitHistories(firstPartVisits, npConf, false) );
//        ArrayList<Visit> secondPartVisits = new ArrayList<>();
//        for(int index = firstPartVisits.size(); index < visits.size(); index ++)
//            secondPartVisits.add(visits.get(index));


        //calculate the alphaGlobal, distance index pairs, standard deviation of each first part visit history beforehand
        //create three has maps for visit histories
        //first, visit history => embedding space alphaGlobal map
        //second, visit history => distance-index-pairs map
        //third, visit history => standard deviation sigma map
        //HashMap<Integer, Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>> firstPartVHAlphaGlobalMap = new HashMap<>();
        //HashMap<Integer, SetMultimap<Double, Integer>> firstPartVHDistIndexPairsMap = new HashMap<>();
        //HashMap<Integer, Double> firstPartVHStdMap = new HashMap<>();
        HashMap<Integer, Integer> firstPartVHLimitFutureStepKMap = new HashMap<>();
        HashMap<Integer, Integer> firstPartVHIndexOfBetaNMap = new HashMap<>();
        HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> firstPartVHNeighborhoodMap = new HashMap<>();


        //max limit future step K to limit all predictions
        int maxLimitFutureStepK = 1;

        //now for each first part visit history calculate distance index pairs, alpha global and std
        for (int index = 0; index < firstPartVisitHistories.size(); index++)
        {
            VisitHistory firstPartVH = firstPartVisitHistories.get(index);

            //location of this visit history
            Integer hashOfVh = firstPartVH.hashCode();

            //time series of this visit history
            ArrayList<TimeSeriesInstant> timeSeriesOfThisVisitHistory = firstPartVH.getTimeSeries();

            //create an embedding space for the time series of this visit history
            Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
                    = algorithm.createEmbeddingSpace(timeSeriesOfThisVisitHistory);
            LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
            int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
            EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);


            //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
            //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
            //where Double.compareTo() method uses long bits of a Double number;
            //we will sort values of this map where indices are in descending order to collect recent indexed neighbors first
            //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]};


            //FOR EPSILON NEIGHBORHOOD WE DO NOT NEED TREE MULTI MAP, LINKED HASH MULTI MAP WOULD BE ENOUGH
            //SetMultimap<Double, Integer> distanceIndexPairs
            //        = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
            //= LinkedHashMultimap.create();
            //iterate over all B_n in alphaGlobal,
            //alphaGlobal contains keys sorted in ascending order
            //alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
            //for (Integer nIndex : alphaGlobal.keySet()) {
            //    double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
            //    distanceIndexPairs.put(distance, nIndex);
            //} // for
            //alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place


            //calculate std
            double stdOfTimeSeriesOfVisitHistory = Math.sqrt(Utils.populationVarianceOfTimeSeries(timeSeriesOfThisVisitHistory));
            LinkedHashMap<Integer, EmbeddingVector> neighborhood = algorithm.epsN(stdOfTimeSeriesOfVisitHistory, alphaGlobal, indexOfBeta_N, beta_N);


            //calculate the limit on future step k
            VisitHistory correspondingSecondPartVH = secondPartVisitHistories.get(index);
            int limitFutureStepK = firstPartVH.maxPossibleK(npConf)
                    > correspondingSecondPartVH.getFrequency()
                    ? correspondingSecondPartVH.getFrequency() : firstPartVH.maxPossibleK(npConf);
            //obtain max limit future step K
            if (limitFutureStepK > maxLimitFutureStepK)
                maxLimitFutureStepK = limitFutureStepK;


            //put the results in corresponding maps
            //firstPartVHAlphaGlobalMap.put(hashOfVh, indexOfBeta_N_alphaGlobalTuple2);
            //firstPartVHDistIndexPairsMap.put(hashOfVh, distanceIndexPairs);
            //firstPartVHStdMap.put(hashOfVh, stdOfTimeSeriesOfVisitHistory);
            firstPartVHIndexOfBetaNMap.put(hashOfVh, indexOfBeta_N);
            firstPartVHNeighborhoodMap.put(hashOfVh, neighborhood);
            firstPartVHLimitFutureStepKMap.put(hashOfVh, limitFutureStepK);
        } // for


        //create global sequence of predicted visits from the first half visit histories,
        //they will be compared against, the second half visit histories of the original visit histories
        TreeSet<Visit> chronologicalSequenceOfPredictedVisitsFromFirstPart
                = chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations(1, algorithm,
                firstPartVisitHistories,
                //firstPartVHAlphaGlobalMap,
                //firstPartVHDistIndexPairsMap,
                //firstPartVHStdMap
                firstPartVHIndexOfBetaNMap,
                firstPartVHNeighborhoodMap
                );

        //map to hold num attempts for this deltaSec
        LinkedHashMap<Long, Integer> numAttemptsConsidered = new LinkedHashMap<>();
        //map to hold numCorrects for this deltaSec
        LinkedHashMap<Long, Integer> numCorrects = new LinkedHashMap<>();

        //for each delta sec initialize maps
        for (long deltaSec : deltaSecs)
        {
            numAttemptsConsidered.put(deltaSec, 0);
            numCorrects.put(deltaSec, 0);
        } // for


        // OLD
        //merge visit histories of the second part
        ArrayList<Visit> secondPartVisits = mergeVisitHistories(secondPartVisitHistories, true);

        //in every iteration of for loop below new number will be generated
        Random random = new Random(seed);

        //for each i, generate a location prediction at T + delta_T and compare it to the real location at T + delta_T
        for (int i = 0; i < numPredictionsToPerform; i++) {
            //long newSeed = newSeed(seed, i + 1);

            //obtain time T and real locations at time T plus delta_T
            Tuple2<Long, LinkedHashMap<Long, Location>> timeTDeltaSecRealLocations
                    = randomTimeTAndRealLocationsAtTimeTPlusDeltaT(algorithm, secondPartVisits, deltaSecs, random, //newSeed
                    0 //errorMarginThetaInSeconds //  do not apply error margin 
            );
            //if timeTDeltaSecRealLocations is null, then there is not enough statistics in the second part
            //i.e. between the first visit and the last visit
            if (timeTDeltaSecRealLocations == null) {
                for (long deltaSec : deltaSecs)
                    deltaSecPredictionPrecisionMap.put(deltaSec, Double.NaN);
                return deltaSecPredictionPrecisionMap; //throw new RuntimeException("Not enough statistics in the second part");
            }

            //now obtain time T and real location locations
            long randomTimeTInDaySeconds = timeTDeltaSecRealLocations._1;
            LinkedHashMap<Long, Location> realLocationsAtTimeTPlusDeltaT = timeTDeltaSecRealLocations._2;

            //for each deltaSec generate the predictions
            for (long deltaSec : deltaSecs)
            {
                Location realLocation = realLocationsAtTimeTPlusDeltaT.get(deltaSec);

                Visit predictedResultVisit = null;

                //do a prediction if real location is non-null
                if (realLocation != null) {
                    //it is possible that chronologicalSequenceOfPredictedVisitsFromFirstPart can be empty for futureStepK = 1 above
                    //we do prediction of that is not empty
                    if (!chronologicalSequenceOfPredictedVisitsFromFirstPart.isEmpty())
                        //pickVisitAtTimeTPlusDeltaTUpToPredLimit can predict up to maxK'
                        predictedResultVisit
                                = algorithm
                                .pickVisitAtTimeTPlusDeltaTOriginal
                                //.pickVisitAtTimeTPlusDeltaTUpToPredLimit
                                (1,
                                        //maxLimitFutureStepK, //maxKOfHighestFrequentFirstPartVisitHistory,
                                        chronologicalSequenceOfPredictedVisitsFromFirstPart, firstPartVisitHistories,
                                        firstPartVHIndexOfBetaNMap,
                                        firstPartVHNeighborhoodMap,
                                        firstPartVHLimitFutureStepKMap,
                                        randomTimeTInDaySeconds, deltaSec, errorMarginThetaInSeconds);
                } // if

                //if(realResultVisit == null) System.out.println("#" +(i + 1) + " realResultVisit => NULL");
                //if(predictedResultVisit == null) System.out.println("#" +(i + 1) + " predictedResultVisit => NULL");


                //return -1 if either realLocation or predictedResultVisit is null
                //if they are both non-null, return 1, if they are equal, otherwise return 0
                if (realLocation != null) {
                    if (predictedResultVisit != null)
                    {
                        //increment attempts
                        int numAttempts = numAttemptsConsidered.get(deltaSec);
                        numAttempts++;
                        numAttemptsConsidered.put(deltaSec, numAttempts);

                        if (realLocation.equals(predictedResultVisit.getLocation())) {
                            int numCorrectPredictions = numCorrects.get(deltaSec);
                            numCorrectPredictions++;
                            numCorrects.put(deltaSec, numCorrectPredictions);
                        } // if
                    } // if
                } // if
            } // for
        } // for


        //now calculate the prediction precisions for all delta seconds
        for (long deltaSec : deltaSecs) {
            //update the map for this deltaSec; prediction precision can also be NaN which will be handled by the caller method
            deltaSecPredictionPrecisionMap.put(deltaSec, numCorrects.get(deltaSec) /
                            (1.0 * numAttemptsConsidered.get(deltaSec))
                    //(1.0 * numPredictionsToPerform)
            );

            //System.out.println( "Prediction precision for a user: " + firstPartVisitHistories.get(0).getUserName()
            //        + " => " + numCorrects.get(deltaSec) / (1.0 * numAttemptsConsidered.get(deltaSec)) );
        } // for

        return deltaSecPredictionPrecisionMap;
    } // predictionPrecisionOfAUserAfterDeltaTInterval


    //helper method to obtain prediction precision with numPredictionsToPerform for a user
    //numPredictionsToPerform values of time T will be generated uniformly at random from [0, 86400] for a given seed;
    //it will return prediction precision for each deltaSec = (5, 15, 30, 60, 120, 240, 480) * 60
    public static LinkedHashMap<Long, Double> predictionPrecisionOfAUserAtRandomTimeT(NextPlaceConfiguration npConf,
                                                                                      ArrayList<Visit> visits,
                                                                                      double trainSplitFraction,
                                                                                      double testSplitFraction,
                                                                                      long[] deltaSecs,
                                                                                      int numPredictionsToPerform,
                                                                                      long seed,
                                                                                      long errorMarginThetaInSeconds)
    {
        if (errorMarginThetaInSeconds < 0)
            throw new RuntimeException("Error margin Theta cannot be negative");

        //hash map to hold delta sec and prediction precision
        LinkedHashMap<Long, Double> deltaSecPredictionPrecisionMap = new LinkedHashMap<>();


        //create two dividable visit histories
        //---------------------  MORE RELIABLE METHOD ---------------------------------------
        int highestFrequency = highestVisitFrequencyOfUser(visits);
        //maxK related to the highest visit frequency cannot be equal to or smaller than 0
        //otherwise user file is not predictable, because future visits
        //cannot be predicted from file's highest frequent visit (location, which will correspond to visit history)
        int maxKOfHighestFrequentVisitHistory = maxValueOfFutureStepK(npConf, highestFrequency);
        if (maxKOfHighestFrequentVisitHistory <= 0) {
            //if it is, then the user's file is not predictable, return 1.0 (which means time series are not predictable)
            //since highest frequent visit (history) is not predictable
            //then all other visits (histories) are not also predictable; which will result predictability error of 1.0;
            //we just omit the calculation here
            System.err.println("Highest Frequent Visit History is not predictable; maxKOfHighestFrequentVisitHistory = "
                    + maxKOfHighestFrequentVisitHistory + "; returning null for Prediction Precision at each T+delta_T");
            return null;
        } // if


        //Here maxKOfHighestFrequentVisitHistoryOfUser is at least 1, so
        //create array list of visit histories to different unique locations;
        //this createVisitHistories() method is more reliable, since
        //visit history of visits whose maxK <= 0 will not be created;
        //only predictable visit histories will be created
        Collection<VisitHistory> visitHistories
                //= createVisitHistories(visits, npConf, true);
                = createVisitHistories(visits, npConf, trainSplitFraction, testSplitFraction);
        //= createVisitHistories(visits, npConf, 8);
        //---------------------  MORE RELIABLE METHOD ---------------------------------------

        NextPlace algorithm = new NextPlace(npConf);

        //visit histories which contain the first half of these visit histories
        ArrayList<VisitHistory> firstPartVisitHistories = new ArrayList<VisitHistory>();

        //visit histories which contain the second half of these visit histories
        ArrayList<VisitHistory> secondPartVisitHistories = new ArrayList<VisitHistory>();

        //loop through all visit histories to generate first half and second half visit histories
        for (VisitHistory visitHistory : visitHistories) {
            //get the first part
            Tuple2<VisitHistory, VisitHistory> firstSecondTuple
                    = visitHistory
                    .divide(npConf, trainSplitFraction, testSplitFraction);
            //.divide(npConf, 8);

            VisitHistory firstHalfVisitHistory = firstSecondTuple._1;
            firstPartVisitHistories.add(firstHalfVisitHistory);


            //now get the second part
            VisitHistory secondHalfVisitHistory = firstSecondTuple._2;
            secondPartVisitHistories.add(secondHalfVisitHistory);
        } // for


        //calculate the alphaGlobal, distance index pairs, standard deviation of each first part visit history beforehand
        //create three has maps for visit histories
        //first, visit history => embedding space alphaGlobal map
        //second, visit history => distance-index-pairs map
        //third, visit history => standard deviation sigma map
        //HashMap<Integer, Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>> firstPartVHAlphaGlobalMap = new HashMap<>();
        //HashMap<Integer, SetMultimap<Double, Integer>> firstPartVHDistIndexPairsMap = new HashMap<>();
        //HashMap<Integer, Double> firstPartVHStdMap = new HashMap<>();
        HashMap<Integer, Integer> firstPartVHLimitFutureStepKMap = new HashMap<>();
        HashMap<Integer, Integer> firstPartVHIndexOfBetaNMap = new HashMap<>();
        HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> firstPartVHNeighborhoodMap = new HashMap<>();

        //max limit future step K to limit all predictions
        int maxLimitFutureStepK = 1;

        //now for each first part visit history calculate distance index pairs, alpha global and std
        for (int index = 0; index < firstPartVisitHistories.size(); index++) {
            VisitHistory firstPartVH = firstPartVisitHistories.get(index);

            //location of this visit history
            Integer hashOfVh = firstPartVH.hashCode();

            //time series of this visit history
            ArrayList<TimeSeriesInstant> timeSeriesOfThisVisitHistory = firstPartVH.getTimeSeries();

            //create an embedding space for the time series of this visit history
            Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
                    = algorithm.createEmbeddingSpace(timeSeriesOfThisVisitHistory);
            LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
            int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
            EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);


            //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
            //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
            //where Double.compareTo() method uses long bits of a Double number;
            //we will sort values of this map where indices are in descending order to collect recent indexed neighbors first
            //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]};


            //FOR EPSILON NEIGHBORHOOD WE DO NOT NEED TREE MULTI MAP, LINKED HASH MULTI MAP WOULD BE ENOUGH
            //SetMultimap<Double, Integer> distanceIndexPairs
            //        = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
            //= LinkedHashMultimap.create();
            //iterate over all B_n in alphaGlobal,
            //alphaGlobal contains keys sorted in ascending order
            //alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
            //for (Integer nIndex : alphaGlobal.keySet()) {
            //    double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, algorithm.getNpConf());
            //    distanceIndexPairs.put(distance, nIndex);
            //} // for
            //alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place


            //calculate std
            double stdOfTimeSeriesOfVisitHistory = Math.sqrt(Utils.populationVarianceOfTimeSeries(timeSeriesOfThisVisitHistory));
            LinkedHashMap<Integer, EmbeddingVector> neighborhood = algorithm.epsN(stdOfTimeSeriesOfVisitHistory, alphaGlobal, indexOfBeta_N, beta_N);

            //calculate the limit on future step k
            VisitHistory correspondingSecondPartVH = secondPartVisitHistories.get(index);
            int limitFutureStepK = firstPartVH.maxPossibleK(npConf)
                    > correspondingSecondPartVH.getFrequency()
                    ? correspondingSecondPartVH.getFrequency() : firstPartVH.maxPossibleK(npConf);
            //obtain max limit future step K
            if (limitFutureStepK > maxLimitFutureStepK)
                maxLimitFutureStepK = limitFutureStepK;


            //put the results in corresponding maps
            //firstPartVHAlphaGlobalMap.put(hashOfVh, indexOfBeta_N_alphaGlobalTuple2);
            //firstPartVHDistIndexPairsMap.put(hashOfVh, distanceIndexPairs);
            //firstPartVHStdMap.put(hashOfVh, stdOfTimeSeriesOfVisitHistory);
            firstPartVHIndexOfBetaNMap.put(hashOfVh, indexOfBeta_N);
            firstPartVHNeighborhoodMap.put(hashOfVh, neighborhood);
            firstPartVHLimitFutureStepKMap.put(hashOfVh, limitFutureStepK);
        } // for


        //create global sequence of predicted visits from the first half visit histories,
        //they will be compared against, the second half visit histories of the original visit histories
        TreeSet<Visit> chronologicalSequenceOfPredictedVisitsFromFirstPart
                = chronologicallyOrderedSequenceOfPredictedVisitsToDifferentLocations(1, algorithm,
                firstPartVisitHistories,
                //firstPartVHAlphaGlobalMap,
                //firstPartVHDistIndexPairsMap,
                //firstPartVHStdMap
                firstPartVHIndexOfBetaNMap,
                firstPartVHNeighborhoodMap
                );

        //System.out.println("chronologicalSequenceOfPredictedVisitsFromFirstPart is empty => "
        //        + chronologicalSequenceOfPredictedVisitsFromFirstPart.isEmpty());

        //find the frequency of the highest frequent visit history
        int frequencyOfHighestFrequentSecondPartVH = highestVisitFrequencyOfUser(secondPartVisitHistories);

        //for each delta sec, we will do numPredictionsToPerform predictions
        for (long deltaSec : deltaSecs)
        {
            //System.out.println("Started deltaMinutes: " + deltaSec / 60 + " ...");

            //initialize the same random number generator with the given seed to reproduce results
            Random random = new Random(seed);

            //when realResultVisit and predictedRealResult is non-null, attempts will be incremented
            int numAttemptsConsidered = 0;
            int numCorrects = 0;

            //for each i, generate a location prediction at T + delta_T and compare it to the real location at T + delta_T
            for (int i = 0; i < numPredictionsToPerform; i++)
            {
                //generate random time T from the interval [0, 86400] uniformly at random;
                //generates random number between 0 (inclusive) and 86401 (exclusive, therefore 86400 is inclusive)
                long randomTimeTInDaySeconds = (long) random.nextInt(86400 + 1);

                //find the real visit at T + delta_T starting futureStepK = 1 => starting index of futureStepK - 1 = 0
                Visit realResultVisit = visitAtTimePlusDeltaTFromTheSecondPart(1,
                        frequencyOfHighestFrequentSecondPartVH, algorithm, secondPartVisitHistories, randomTimeTInDaySeconds,
                        deltaSec, errorMarginThetaInSeconds);


                Visit predictedResultVisit = null;

                //do a prediction id real result visit is non-null
                if (realResultVisit != null) {
                    //it is possible that chronologicalSequenceOfPredictedVisitsFromFirstPart can be empty for futureStepK = 1 above
                    //we do prediction of that is not empty
                    if (!chronologicalSequenceOfPredictedVisitsFromFirstPart.isEmpty())
                        // pickVisitAtTimeTPlusDeltaTUpToPredLimit predicts up to maxK
                        predictedResultVisit
                                = algorithm.pickVisitAtTimeTPlusDeltaTOriginal
                                //pickVisitAtTimeTPlusDeltaTUpToPredLimit
                                (1,
                                        //maxLimitFutureStepK, //maxKOfHighestFrequentFirstPartVisitHistory,
                                        chronologicalSequenceOfPredictedVisitsFromFirstPart, firstPartVisitHistories,
                                        //firstPartVHAlphaGlobalMap,
                                        //firstPartVHDistIndexPairsMap,
                                        //firstPartVHStdMap,
                                        firstPartVHIndexOfBetaNMap,
                                        firstPartVHNeighborhoodMap,
                                        firstPartVHLimitFutureStepKMap,
                                        randomTimeTInDaySeconds, deltaSec, errorMarginThetaInSeconds);
                } // if
                //System.out.println("|| predictedResultVisit: " + (predictedResultVisit == null ? null : predictedResultVisit.toStringWithDate()));

                //if(realResultVisit == null) System.out.println("#" +(i + 1) + " realResultVisit => NULL");
                //if(predictedResultVisit == null) System.out.println("#" +(i + 1) + " predictedResultVisit => NULL");

                //return -1 if either realResultVisit or predictedResultVisit is null
                //if they are both non-null, return 1, if they are equal, otherwise return 0
                if (realResultVisit != null) {
                    if (predictedResultVisit != null) {
                        //increment attempts
                        numAttemptsConsidered++;

                        if (realResultVisit.getLocation().equals(predictedResultVisit.getLocation()))
                            numCorrects++;
                    } // if
                } // if
            } // for

            //double predictionPrecision = numCorrects / (1.0 * numAttemptsConsidered);
            //if(Double.isNaN(predictionPrecision)) predictionPrecision = 0;


            //update the map for this deltaSec; prediction precision can also be NaN which will be handled by the caller method
            deltaSecPredictionPrecisionMap.put(deltaSec, numCorrects /
                            (1.0 * numAttemptsConsidered)
                    //(1.0 * numPredictionsToPerform)
            );

            //System.out.println("Ended deltaMinutes: " + deltaSec / 60 + " ...\n");
        } // for


        return deltaSecPredictionPrecisionMap;
    } // predictionPrecisionOfAUserAtRandomTimeT


    //helper method to merge visit histories together to obtain chronologically ordered visits
    public static ArrayList<Visit> mergeVisitHistories(Collection<VisitHistory> visitHistories, final boolean withTimeOfDayOrder) {
        TreeSet<Visit> resultVisits = new TreeSet<>();
        visitHistories.forEach(vh ->
        {
            if (withTimeOfDayOrder)  // day seconds ordering
                for (Visit v : vh.getVisits()) {
                    resultVisits.add(Visit.newInstanceWithUnixEpochDate(v));
                } // for
            else resultVisits.addAll(vh.getVisits()); // chronological ordering
        });

        return new ArrayList<>(resultVisits);
    } // mergeVisitHistories


    //helper method to pick a time T uniformly at random from the second part of user visits
    public static Tuple2<Long, LinkedHashMap<Long, Location>> randomTimeTAndRealLocationsAtTimeTPlusDeltaT(NextPlace algorithm,
                                                                                                           List<Visit> secondPartVisits,
                                                                                                           long[] deltaSecs
                                                                                                            ,Random random
                                                                                                            //, long seed
                                                                                                            ,long errorMarginThetaInSeconds)
    {
        if (secondPartVisits.isEmpty())
        {
            //there should be at least one visit in secondPartVisits to pick a time t
            return null;
        } // if
        if (deltaSecs.length == 0) {
            throw new RuntimeException("DeltaSecs cannot be empty");
        } // if

        //we also assume that all visits are sorted in chronological order
        //we assume that deltaSecs is sorted in ascending order
        //so, number of seconds between first visit start time and last visit edn time should be less than
        //or equal to deltaSecs[deltaSecs.length - 1], otherwise return null
//        Visit firstVisit = secondPartVisits.get(0);
//        Date firstVisitStartTime = firstVisit.getTimeSeriesInstant().getArrivalTime();
//
//        //obtain start time and end time for the last visit
//        Visit lastVisit = secondPartVisits.get(secondPartVisits.size() - 1);
//        Date lastVisitStartTime = lastVisit.getTimeSeriesInstant().getArrivalTime();
//        Date lastVisitEndTime = Utils.addSeconds(lastVisitStartTime,
//                lastVisit.getTimeSeriesInstant().getResidenceTimeInSeconds());
//        if(secondPartVisits.size() == 1)
//        {
//            Date firstVisitEndTime = Utils.addSeconds(firstVisitStartTime,
//                    firstVisit.getTimeSeriesInstant().getResidenceTimeInSeconds());
//            if(noOfSecondsBetween
//                    (
//                            Utils.subtractSeconds(firstVisitStartTime, errorMarginThetaInSeconds), //firstVisitStartTime,
//                            Utils.addSeconds(firstVisitEndTime, errorMarginThetaInSeconds) //firstVisitEndTime
//                    )
//                    < deltaSecs[deltaSecs.length - 1])
//                return null; //no statistics is available after deltaSecs[deltaSecs.length - 1] seconds, return null
//        } // if
//        else //secondPartVisits.size() > 1
//        {
//            if(noOfSecondsBetween
//                    (
//                            Utils.subtractSeconds(firstVisitStartTime, errorMarginThetaInSeconds), //firstVisitStartTime,
//                            Utils.addSeconds(lastVisitEndTime, errorMarginThetaInSeconds) //lastVisitEndTime
//                    )
//                    < deltaSecs[deltaSecs.length - 1])
//                return null; //no statistics is available after deltaSecs[deltaSecs.length - 1] seconds, return null
//        } // else


        // you can also test this
        //get visit at random time T
        //long randomTimeTInDaySeconds = randomTimeTInDaySeconds(seed);


        //define new random number generator
        //Random random = new Random(seed);


        //we will randomly pick a visit from secondPartVisits and check that if there are statistics up to last visit
        //if not we will pick again
        int randIndex = random.nextInt(secondPartVisits.size());
        Visit randomVisit = secondPartVisits.get(randIndex);
//        Date randomVisitStartTime = randomVisit.getTimeSeriesInstant().getArrivalTime();
//        while(noOfSecondsBetween
//                (
//                Utils.subtractSeconds(randomVisitStartTime, errorMarginThetaInSeconds), //randomVisitStartTime,
//                Utils.addSeconds(lastVisitEndTime, errorMarginThetaInSeconds) //lastVisitEndTime
//                ) < deltaSecs[deltaSecs.length - 1])
//        {
//            //generate a new random visit
//            randIndex = random.nextInt(secondPartVisits.size());
//            randomVisit = secondPartVisits.get(randIndex);
//            randomVisitStartTime = randomVisit.getTimeSeriesInstant().getArrivalTime();
//        } // while


        //at the end of whole loop we will obtain a random visit where after deltaSecs[deltaSecs.length - 1]
        //there will still be some statistics;
        //now obtain random time T
        long randomTimeT = randomVisit.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
        //now for each delta_T obtain a real location at T+delta_T
        //collect them in a map
        LinkedHashMap<Long, Location> locationsAtTPlusDeltaT = new LinkedHashMap<>();
        for (long deltaSec : deltaSecs) {
            //Date randomTimeTPlusDeltaT = Utils.addSeconds(randomVisitStartTime, deltaSec);
            Location locationAtTPlusDeltaT = null; // as a starting point
            //now for each visit after randIndex, check whether it contains randomTimeTPlusDeltaT
            for (int visitIndex = randIndex; visitIndex < secondPartVisits.size(); visitIndex++) {
                Visit v = secondPartVisits.get(visitIndex);
                //obtain corresponding visit's start time and end time
                //Date startTimeOfAVisitAtVisitIndex = v.getTimeSeriesInstant().getArrivalTime();
                //Date endTimeOfAVisitAtVisitIndex
                //        = Utils.addSeconds(startTimeOfAVisitAtVisitIndex, v.getTimeSeriesInstant().getResidenceTimeInSeconds());
                //add error margin for both of them
                //startTimeOfAVisitAtVisitIndex = Utils.subtractSeconds(startTimeOfAVisitAtVisitIndex, errorMarginThetaInSeconds);
                //endTimeOfAVisitAtVisitIndex = Utils.addSeconds(endTimeOfAVisitAtVisitIndex, errorMarginThetaInSeconds);


                //start time <= T+delta_T <= end time
                //if((startTimeOfAVisitAtVisitIndex.before(randomTimeTPlusDeltaT) || startTimeOfAVisitAtVisitIndex.equals(randomTimeTPlusDeltaT))
                //        && (endTimeOfAVisitAtVisitIndex.after(randomTimeTPlusDeltaT) || endTimeOfAVisitAtVisitIndex.equals(randomTimeTPlusDeltaT))
                //    )
                if (algorithm.isInInterval(v, randomTimeT, deltaSec, errorMarginThetaInSeconds)) {
                    locationAtTPlusDeltaT = v.getLocation();
                    //location is found, break
                    break;
                } // if
            } // for

            //after above for loop it is possible that a location is found or no location found indicating
            //that user will not be in any significant location (null), i.e. transitioning between them;
            //update the map
            locationsAtTPlusDeltaT.put(deltaSec, locationAtTPlusDeltaT);
        } // for


        return new Tuple2<>(randomTimeT, locationsAtTPlusDeltaT);
    } // randomTimeTAndRealLocationsAtTimeTPlusDeltaT


    //helper method to calculate the prediction precision of the dataset in a distributed fashion
    public static Tuple2<LinkedHashMap<Long, Integer>, LinkedHashMap<Long, Double>> distributedPredictionPrecisionOfADataset(Dataset dataset,
                                                                                                                             JavaSparkContext sc,
                                                                                                                             List<String> ppUserFilePaths,
                                                                                                                             NextPlaceConfiguration npConf,
                                                                                                                             double trainSplitFraction,
                                                                                                                             double testSplitFraction,
                                                                                                                             long[] deltaSecs,
                                                                                                                             int numPredictionsToPerformForEachUser,
                                                                                                                             long seedToGenerateTimeT,
                                                                                                                             long errorMarginThetaInSeconds)
    {
        System.out.println("Calculating Prediction Precision for " + dataset + " in a distributed fashion...");

        //broadcast variables
        final Broadcast<SerializableWritable<Configuration>> broadcastConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));
        final Broadcast<NextPlaceConfiguration> broadcastNpConf = sc.broadcast(npConf);
        final Broadcast<Double> broadcastTrainSplitFraction = sc.broadcast(trainSplitFraction);
        final Broadcast<Double> broadcastTestSplitFraction = sc.broadcast(testSplitFraction);
        final Broadcast<Integer> broadcastNumPredictionsToPerformForEachUser = sc.broadcast(numPredictionsToPerformForEachUser);
        final Broadcast<Long> broadcastSeedToGenerateTimeT = sc.broadcast(seedToGenerateTimeT);
        final Broadcast<Long> broadcastErrorMarginThetaInSeconds = sc.broadcast(errorMarginThetaInSeconds);
        final Broadcast<long[]> broadcastDeltaSecs = sc.broadcast(deltaSecs);


        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, ppUserFilePaths.size());
        JavaPairRDD<String, LinkedHashMap<Long, Double>> userDeltaSecPredictionPrecisionRDD = ppUserFilePathsRDD.mapToPair(
                new PairFunction<String, String, LinkedHashMap<Long, Double>>() {
                    private static final long serialVersionUID = -8089390749857311852L;

                    @Override
                    public Tuple2<String, LinkedHashMap<Long, Double>> call(String ppUserFilePath) throws Exception {
                        InputStream in = inputStreamFromHDFSFilePath(broadcastConf.value().value(), ppUserFilePath);
                        ArrayList<Visit> significantVisits = visitsFromPPUserFileInputStream(in);

                        LinkedHashMap<Long, Double> deltaSecPredictionPrecisionOfThisUser
                                //= predictionPrecisionOfAUserAtRandomTimeT
                                = predictionPrecisionOfAUserAfterDeltaTInterval
                                (broadcastNpConf.value(),
                                        significantVisits,
                                        broadcastTrainSplitFraction.value(),
                                        broadcastTestSplitFraction.value(),
                                        broadcastDeltaSecs.value(),
                                        broadcastNumPredictionsToPerformForEachUser.value(),
                                        broadcastSeedToGenerateTimeT.value(),
                                        broadcastErrorMarginThetaInSeconds.value());

                        return new Tuple2<>(ppUserFilePath, deltaSecPredictionPrecisionOfThisUser);
                    } // call
                });


        //user and deltaSecPredictionPrecisionMap
        Map<String, LinkedHashMap<Long, Double>> userDeltaSecPredictionPrecisionMap
                = userDeltaSecPredictionPrecisionRDD.collectAsMap();

        ppUserFilePathsRDD.unpersist();
        ppUserFilePathsRDD = null;
        userDeltaSecPredictionPrecisionRDD.unpersist();
        userDeltaSecPredictionPrecisionRDD = null;
        broadcastConf.unpersist();
        broadcastNpConf.unpersist();
        broadcastTrainSplitFraction.unpersist();
        broadcastTestSplitFraction.unpersist();
        broadcastNumPredictionsToPerformForEachUser.unpersist();
        broadcastSeedToGenerateTimeT.unpersist();
        broadcastErrorMarginThetaInSeconds.unpersist();


        //now calculate the prediction precision of the dataset
        LinkedHashMap<Long, Integer> deltaSecNumberOfUsersConsidered = new LinkedHashMap<>();
        LinkedHashMap<Long, Double> deltaSecSumOfPP = new LinkedHashMap<>();
        for(long deltaSec : deltaSecs)
        {
            deltaSecNumberOfUsersConsidered.put(deltaSec, 0);
            deltaSecSumOfPP.put(deltaSec, 0.0);
        } // for


        for (String ppUserFilePath : userDeltaSecPredictionPrecisionMap.keySet())
        {
            LinkedHashMap<Long, Double> deltaSecPredictionPrecisionMap
                    = userDeltaSecPredictionPrecisionMap.get(ppUserFilePath);

            for (Long deltaSec : deltaSecPredictionPrecisionMap.keySet()) {
                //System.out.println("Prediction Precision at T + " + deltaSec / 60 + "min => "
                //        + String.format("%.2f", deltaSecPredictionPrecisionMap.get(deltaSec) * 100) + "%");
                double pp = deltaSecPredictionPrecisionMap.get(deltaSec);
                double sum = deltaSecSumOfPP.get(deltaSec);
                int numberOfUsersConsidered = deltaSecNumberOfUsersConsidered.get(deltaSec);
                if (!Double.isNaN(pp)) {
                    sum += pp;
                    numberOfUsersConsidered++;
                } // if
                deltaSecSumOfPP.put(deltaSec, sum);
                deltaSecNumberOfUsersConsidered.put(deltaSec, numberOfUsersConsidered);
            } // for

            //do it for one user now
            //break;
        } // for


        System.out.println("Number of users: " + ppUserFilePaths.size());
//        for(Long deltaSec : deltaSecNumberOfUsersConsidered.keySet())
//            System.out.println("deltaMinutes: " + deltaSec / 60 + " => number of users considered: "
//                    + deltaSecNumberOfUsersConsidered.get(deltaSec));
//        for(Long deltaSec : deltaSecSumOfPP.keySet())
//            System.out.println("Prediction Precision of " + dataset +  " at T + " + deltaSec / 60 + "min => "
//                    + String.format("%.2f", deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec) ) + "%");


        System.out.println("\n");
        for (Long deltaSec : deltaSecSumOfPP.keySet()) {
            switch (deltaSec.intValue())
            {
                case 5 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 0 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 15 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 20 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 30 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 40 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 60 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 60 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 120 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 80 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 240 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 100 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 480 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 120 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
            } // switch
        } // for


        // reset all values to the number of users considered for the first delta seconds
        int globalNumberOfUsersConsidered = deltaSecNumberOfUsersConsidered.get(5 * 60L);
//        for(long deltaSec : deltaSecSumOfPP.keySet())
//        {
//            deltaSecNumberOfUsersConsidered.put(deltaSec, globalNumberOfUsersConsidered);
//        } // for


        //prediction precision of the dataset for each delta sec
        LinkedHashMap<Long, Double> deltaSecPredictionPrecisionOfTheDatasetMap = new LinkedHashMap<>();
        for (Long deltaSec : deltaSecSumOfPP.keySet()) {
            deltaSecPredictionPrecisionOfTheDatasetMap.put(deltaSec,
                    deltaSecSumOfPP.get(deltaSec) * 100 /
                            //deltaSecNumberOfUsersConsidered.get(deltaSec)
                            globalNumberOfUsersConsidered
            );
        } // for


        return new Tuple2<>(deltaSecNumberOfUsersConsidered, deltaSecPredictionPrecisionOfTheDatasetMap);
    } // distributedPredictionPrecisionOfADataset


    //helper method to calculate prediction precision of the dataset in a serial fashion
    public static Tuple2<LinkedHashMap<Long, Integer>, LinkedHashMap<Long, Double>> predictionPrecisionOfADataset(Dataset dataset,
                                                                                                                  List<String> ppUserFilePaths,
                                                                                                                  NextPlaceConfiguration npConf,
                                                                                                                  double trainSplitFraction,
                                                                                                                  double testSplitFraction,
                                                                                                                  long[] deltaSecs,
                                                                                                                  int numPredictionsToPerformForEachUser,
                                                                                                                  long seedToGenerateTimeT,
                                                                                                                  long errorMarginThetaInSeconds)
    {
        System.out.println("Calculating Prediction Precision for " + dataset + " in a serial fashion...");

        //now calculate the prediction precision of the dataset
        LinkedHashMap<Long, Integer> deltaSecNumberOfUsersConsidered = new LinkedHashMap<>();
        LinkedHashMap<Long, Double> deltaSecSumOfPP = new LinkedHashMap<>();
        for(long deltaSec : deltaSecs)
        {
            deltaSecNumberOfUsersConsidered.put(deltaSec, 0);
            deltaSecSumOfPP.put(deltaSec, 0.0);
        } // for


        for(String ppUserFilePath : ppUserFilePaths)
        {
            InputStream in = inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> significantVisits = visitsFromPPUserFileInputStream(in);

            LinkedHashMap<Long, Double> deltaSecPredictionPrecisionOfThisUser
                    //= predictionPrecisionOfAUserAtRandomTimeT
                    = predictionPrecisionOfAUserAfterDeltaTInterval
                    (npConf,
                            significantVisits,
                            trainSplitFraction,
                            testSplitFraction,
                            deltaSecs,
                            numPredictionsToPerformForEachUser,
                            seedToGenerateTimeT,
                            errorMarginThetaInSeconds);

            for (Long deltaSec : deltaSecPredictionPrecisionOfThisUser.keySet())
            {
                //System.out.println("Prediction Precision at T + " + deltaSec / 60 + "min => "
                //        + String.format("%.2f", deltaSecPredictionPrecisionOfThisUser.get(deltaSec) * 100) + "%");
                double pp = deltaSecPredictionPrecisionOfThisUser.get(deltaSec);
                double sum = deltaSecSumOfPP.get(deltaSec);
                int numberOfUsersConsidered = deltaSecNumberOfUsersConsidered.get(deltaSec);
                if (!Double.isNaN(pp)) {
                    sum += pp;
                    numberOfUsersConsidered++;
                } // if
                deltaSecSumOfPP.put(deltaSec, sum);
                deltaSecNumberOfUsersConsidered.put(deltaSec, numberOfUsersConsidered);
            } // for

            //do it for one user now
            //break;
        } // for


        System.out.println("Number of users: " + ppUserFilePaths.size());
//        for(Long deltaSec : deltaSecNumberOfUsersConsidered.keySet())
//            System.out.println("deltaMinutes: " + deltaSec / 60 + " => number of users considered: "
//                    + deltaSecNumberOfUsersConsidered.get(deltaSec));
//        for(Long deltaSec : deltaSecSumOfPP.keySet())
//            System.out.println("Prediction Precision of " + dataset +  " at T + " + deltaSec / 60 + "min => "
//                    + String.format("%.2f", deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec) ) + "%");


        System.out.println("\n");
        for (Long deltaSec : deltaSecSumOfPP.keySet()) {
            switch (deltaSec.intValue())
            {
                case 5 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 0 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 15 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 20 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 30 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 40 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 60 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 60 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 120 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 80 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 240 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 100 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
                case 480 * 60:
                    System.out.println((deltaSecNumberOfUsersConsidered.get(deltaSec))
                            + "," + 120 + "," + String.format("%.2f",
                            deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "\\\\"
                            + " %" + deltaSec.intValue() / 60 + " minutes");
                    break;
            } // switch
        } // for


        // reset all values to the number of users considered for the first delta seconds
        int globalNumberOfUsersConsidered = deltaSecNumberOfUsersConsidered.get(5 * 60L);
//        for(long deltaSec : deltaSecSumOfPP.keySet())
//        {
//            deltaSecNumberOfUsersConsidered.put(deltaSec, globalNumberOfUsersConsidered);
//        } // for


        //prediction precision of the dataset for each delta sec
        LinkedHashMap<Long, Double> deltaSecPredictionPrecisionOfTheDatasetMap = new LinkedHashMap<>();
        for (Long deltaSec : deltaSecSumOfPP.keySet()) {
            deltaSecPredictionPrecisionOfTheDatasetMap.put(deltaSec,
                    deltaSecSumOfPP.get(deltaSec) * 100 /
                            //deltaSecNumberOfUsersConsidered.get(deltaSec)
                            globalNumberOfUsersConsidered
            );
        } // for


        return new Tuple2<>(deltaSecNumberOfUsersConsidered, deltaSecPredictionPrecisionOfTheDatasetMap);
    } // predictionPrecisionOfADataset


    //helper method to find a location at time T + delta_T for the real part of the visit histories;
    //namely, first part will be used to generate the location prediction at time T + delta_T and this predicted location
    //will be compared to the location at time T + delta_T obtained from the second part
    public static Visit visitAtTimePlusDeltaTFromTheSecondPart(int futureStepK,
                                                               int frequencyOfHighestFrequentVH,
                                                               NextPlace algorithm,
                                                               Collection<VisitHistory> secondPartVisitHistories,
                                                               long timeTInDaySeconds,
                                                               long deltaTSeconds,
                                                               long errorMarginThetaInSeconds)
    {
        if (futureStepK <= 0)
            throw new RuntimeException("Provided future step K cannot smaller than or equal to 0.");


//        //for each visit history, collect visit at the index = futureStepK - 1, which basically corresponds to predicted visit
//        //at futureStepK for respective first part visit history;
//        //collected visits date part should be set to unix epoch to make them comparable with arrival time in day seconds
//        TreeSet<Visit> sequenceOfCollectedVisits = new TreeSet<>();
//
//        //for each visit history obtain visit at index futureStepK - 1
//        //and provided futureStepK cannot exceed number of visits that second part visit history contains
//        for(VisitHistory vh : secondPartVisitHistories)
//        {
//            if(futureStepK <= vh.getFrequency()) // if futureStepK does not exceed number of visits in this vh
//            {
//                Visit v = vh.getVisits().get(futureStepK - 1);
//
//                //transform v to visit with date equal to Unix Epoch
//                Date unixEpochDateWithArrivalTimeInDaySeconds
//                        = Utils.unixEpochDateWithDaySeconds(v.getTimeSeriesInstant().getArrivalTimeInDaySeconds());
//                Visit unixEpochVisit = new Visit(v.getUserName(), v.getLocation(),
//                        new TimeSeriesInstant(unixEpochDateWithArrivalTimeInDaySeconds,
//                                v.getTimeSeriesInstant().getResidenceTimeInSeconds()));
//
//                sequenceOfCollectedVisits.add(unixEpochVisit);
//            } // if
//        } // for
//
//        //now search a visit which is performed at T + delta_T
//        ArrayList<Visit> pickedVisitsAtTimePlusDeltaT = new ArrayList<>();
//        for(Visit v : sequenceOfCollectedVisits)
//        {
//            //if v contain T + delta_T, then add it to pickedVisitsAtTimePlusDeltaT
//            if(algorithm.isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
//                pickedVisitsAtTimePlusDeltaT.add(v);
//        } // for
//
//        //if only one visit is found, return it
//        if(pickedVisitsAtTimePlusDeltaT.size() == 1)
//            return pickedVisitsAtTimePlusDeltaT.get(0);
//        else if(pickedVisitsAtTimePlusDeltaT.size() > 1) // more than one visit is contained, then return random visit
//            return randomVisit(pickedVisitsAtTimePlusDeltaT);
//        else // pickedVisitsAtTimePlusDeltaT is empty, update futureStepK and repeat the procedure
//        {
//            //the recursion will stop if futureStepK is bigger or equal to highest visit frequency
//            //or when some visit is returned;
//            if(futureStepK < frequencyOfHighestFrequentVH)
//            {
//                 //increment futureStepK and look for a new visit
//                int incrementedFutureStepK = futureStepK + 1;
//
//                //recursively obtain a visit
//                return visitAtTimePlusDeltaTFromTheSecondPart(incrementedFutureStepK, frequencyOfHighestFrequentVH,
//                                    algorithm, secondPartVisitHistories, timeTInDaySeconds, deltaTSeconds,
//                                    errorMarginThetaInSeconds);
//            } // if
//            else // pickedVisitsAtTimePlusDeltaT is empty
//            {
//                return null; //the search limit is reached and no visit is found at T + delta_T
//            } // else
//
//        } // else


        //repeat until the limit futureStepK becomes > frequencyOfHighestFrequentVH
        while (futureStepK <= frequencyOfHighestFrequentVH) {
            //for each visit history, collect visit at the index = futureStepK - 1, which basically corresponds to predicted visit
            //at futureStepK for respective first part visit history;
            //collected visits date part should be set to unix epoch to make them comparable with arrival time in day seconds
            TreeSet<Visit> sequenceOfCollectedVisits = new TreeSet<>();

            //for each visit history obtain visit at index futureStepK - 1
            //and provided futureStepK cannot exceed number of visits that second part visit history contains
            for (VisitHistory vh : secondPartVisitHistories) {
                if (futureStepK <= vh.getFrequency()) // if futureStepK does not exceed number of visits in this vh
                {
                    Visit v = vh.getVisits().get(futureStepK - 1);

                    //transform v to visit with Unix Epoch date
                    //Visit unixEpochVisit = Visit.newInstanceWithUnixEpochDate(v);

                    sequenceOfCollectedVisits.add(v);
                } // if
            } // for

            //now search a visit which is performed at T + delta_T
            ArrayList<Visit> pickedVisitsAtTimePlusDeltaT = new ArrayList<>();
            for (Visit v : sequenceOfCollectedVisits) {
                //if v contain T + delta_T, then add it to pickedVisitsAtTimePlusDeltaT
                if (algorithm.isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
                    pickedVisitsAtTimePlusDeltaT.add(v);
            } // for

            //if only one visit is found, return it
            if (pickedVisitsAtTimePlusDeltaT.size() == 1)
                return pickedVisitsAtTimePlusDeltaT.get(0);
            else if (pickedVisitsAtTimePlusDeltaT.size() > 1) // more than one visit is contained, then return random visit
                return randomVisit(pickedVisitsAtTimePlusDeltaT); //  non-random visit can be returned
            else // pickedVisitsAtTimePlusDeltaT is empty
            {
                //else increment futureStepK and perform the search again
                futureStepK++; //  for the original next place algorithm futureStepK can be doubled
            } // else
        } // while

        //if while loop finishes and nothing is returned, then return null
        //since search is performed up to the limit
        return null;
    } // visitAtTimePlusDeltaTFromTheSecondPart


    //helper method to return random visit from list of visits
    public static Visit randomVisit(List<Visit> visits)
    {
        //return one element from array list randomly;
        //nextInt() method returns a pseudo-random, uniformly distributed int value between 0 (inclusive)
        //and the specified value (exclusive).
        int randIndex = ThreadLocalRandom.current().nextInt(visits.size());
        return visits.get(randIndex);
    } // randomVisit


    public static Visit randomVisitWithHighestResidenceSeconds(ArrayList<Visit> visits)
    {
        long highestResSeconds = 0;
        int highestResSecondsIndex = 0;

        for(int index = 0; index < visits.size(); index ++)
        {
            long thisVisitResSeconds = visits.get(index).getTimeSeriesInstant().getResidenceTimeInSeconds();
            if(thisVisitResSeconds > highestResSeconds)
            {
                highestResSeconds = thisVisitResSeconds;
                highestResSecondsIndex = index;
            } // if
        } // for

        return visits.get(highestResSecondsIndex);
    } // randomVisitWithHighestResidenceSeconds


//    //helper method to return random visit, whose location is highest frequent and/or
//    //its corresponding visit history's total residence seconds is also highest
//    public static Visit randomVisitWithHighestFrequencyAndTotalResSeconds(ArrayList<Visit> visits)
//    {
//        if (visits == null || visits.isEmpty())
//            throw new RuntimeException("Visits list is either null or empty");
//
//
//        //at this point visits list has at least one visit
//        int highestFrequencyInVisits = highestVisitFrequencyOfUser(visits);
//        //create visit histories with highest frequency
//        Collection<VisitHistory> resultVisitHistories
//                = Utils.createVisitHistories(visits, highestFrequencyInVisits);
//
//
//        //WORSE THAN below original implementation
//        //NEW
//        //List<Visit> resultVisits = new ArrayList<Visit>();
//        //for(VisitHistory vh : resultVisitHistories)
//        //    resultVisits.addAll(vh.getVisits());
//        //return randomVisit(resultVisits);
//        //NEW
//
//
//        //initialize maxTotalResSecVisitHistory with the first visit history in the array list
//        //resultVisitHistories has at least 1 element, because after above if => visits.size() >= 1
//        VisitHistory maxTotalResSecVisitHistory = resultVisitHistories.iterator().next();
//
//
//        //if resultVisitHistories.size() = 1, the only visit history in the list is maxTotalResSecVisitHistory
//        if (resultVisitHistories.size() == 1)
//            return randomVisit(maxTotalResSecVisitHistory.getVisits());
//        else // resultVisitHistories.size() > 1
//        {
//            //max total res seconds resultVisitHistories >= 1;
//            //we will get visit history with higher total residence seconds and return random visit from  it
//            long maxTotalResSeconds = 0;
//
//            //obtain visit history with highest residence seconds and return a random visit from it
//            for (VisitHistory resultVisitHistory : resultVisitHistories) {
//                if (resultVisitHistory.getTotalResidenceSeconds() >= maxTotalResSeconds) {
//                    maxTotalResSeconds = resultVisitHistory.getTotalResidenceSeconds();
//                    maxTotalResSecVisitHistory = resultVisitHistory;
//                } // if
//            } // for
//
//            return randomVisit(maxTotalResSecVisitHistory.getVisits());
//        } // else
//
//    } // randomVisitWithHighestFrequencyAndTotalResSeconds


    //helper method to get the visit by location
    public static ArrayList<Visit> getVisitsByLocation(List<Visit> visits, Location location)
    {
        ArrayList<Visit> locationVisits = new ArrayList<Visit>();
        for (Visit v : visits) {
            if (v.getLocation().equals(location))
                locationVisits.add(v);
        } // for

        return locationVisits;
    } // getVisitsByLocation


    //helper method to generate randomTimeTInDaySeconds, deltaTSeconds will be added later in the caller methods
    public static long randomTimeTInDaySeconds(long seed)
    {
        Random random = new Random(seed);
        //returns random number between 0 (inclusive) and 86401 (exclusive, therefore 86400 is inclusive)
        return (long) random.nextInt(86400 + 1);
    } // randomTimeTInDaySeconds


    //overloaded method to generate timeTInDaySeconds with random seed
    //noOfPredictions parameter will help to generate the same noOfPredictions number of timeTInDaySeconds every time
    public static long randomTimeTInDaySeconds(long seed, int noOfPredictions)
    {
        assert noOfPredictions >= 1;

        long prime = 31;
        long resultSeed = 17;
        for (int count = 0; count < noOfPredictions; count++)
            resultSeed = prime * resultSeed + seed;

        return randomTimeTInDaySeconds(resultSeed);
    } // randomTimeTPlusDeltaTInDaySeconds


    private static long newSeed(long seed, int noOfPredictions)
    {
        long prime = 31;
        long resultSeed = 17;
        for (int count = 0; count < noOfPredictions; count++)
            resultSeed = prime * resultSeed + seed;

        return resultSeed;
    } // newSeed


    //helper method to generate (randomTimeTPlusDeltaTInDaySeconds)
    //timeT_PInDaySeconds = timeTInDaySeconds + deltaTSeconds with a random seed
    public static long randomTimeTPlusDeltaTInDaySeconds(long deltaTSeconds, long seed)
    {
        //perform sum mod 86400
        return (randomTimeTInDaySeconds(seed) + deltaTSeconds) % 86400;
    } // randomTimeTPlusDeltaTInDaySeconds


    //overloaded method to generate timeT_PInDaySeconds (randomTimeTPlusDeltaTInDaySeconds) with random seed
    //noOfPredictions parameter will help to generate the same noOfPredictions number of timeT_PInDaySeconds every time
    public static long randomTimeTPlusDeltaTInDaySeconds(long deltaTSeconds, long seed, int noOfPredictions)
    {
        assert noOfPredictions >= 1;

        long prime = 31;
        long resultSeed = 17;
        for (int count = 0; count < noOfPredictions; count++)
            resultSeed = prime * resultSeed + seed;

        return randomTimeTPlusDeltaTInDaySeconds(deltaTSeconds, resultSeed);
    } // randomTimeTPlusDeltaTInDaySeconds

    //overloaded version without random seed
    public static long randomTimeTPlusDeltaTInDaySeconds(long deltaTSeconds)
    {
        //nextInt() method returns a pseudo-random, uniformly distributed int value between 0 (inclusive)
        //and the specified value (exclusive);
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        long randomTimeTInDaySeconds = ThreadLocalRandom.current().nextInt(86400 + 1);

        //perform sum mod 86400
        return (randomTimeTInDaySeconds + deltaTSeconds) % 86400;
    } // randomTimeTPlusDeltaTInDaySeconds


    //method to calculate the variance sigma^2 of the given time series
    //which is the array list of time series instants
    public static double populationVarianceOfTimeSeries(ArrayList<TimeSeriesInstant> timeSeries)
    {
        //WE ASSUME THAT given timeSeries are sorted chronologically, therefore
        //do not execute the following method
        //Collections.sort(timeSeries);

        //variance calculation done by non-linear time series book
        /*
        Author: Rainer Hegger Last modified: May 23th, 1998
        #include <stdio.h>
        #include <stdlib.h>
        #include <math.h>

        void variance(double *s,unsigned long l,double *av,double *var)
        {
            unsigned long i;
            double h;

            *av= *var=0.0;

            for (i=0;i<l;i++) {
                h=s[i];
                *av += h;
                *var += h*h;
            }

            *av /= (double)l;
            *var=sqrt(fabs((*var)/(double)l-(*av)*(*av)));

            if (*var == 0.0)
            {
                fprintf(stderr,"Variance of the data is zero. Exiting!\n\n");
                exit(VARIANCE_VAR_EQ_ZERO);
            }
        }
        */


        //mean (t, d) is used by non-linear time series book;
        //median of time series is tested with media absolute deviation and does not produce good results
        int timeSeriesInstantCount = timeSeries.size();

        //if instant count is 0, then no variation in the time series, return 0
        //if instant count is 1, then again variance is 0
        if (timeSeriesInstantCount == 0 || timeSeriesInstantCount == 1) return 0;


        /*
        double avg = 0;
        double var = 0;
        for(TimeSeriesInstant instant : timeSeries)
        {
            double h = instant.getArrivalTimeInDaySeconds();
            avg += h;
            var += h * h;
        } // for
        avg /= timeSeriesInstantCount;

        var = Math.abs((var / timeSeriesInstantCount) - avg * avg);
        return var;
        */


        //-------------------------------- NEW -------------------------------------
        //the mean instant, whose arrival time in day seconds and residence seconds are average of all instants in timeSeries
        TimeSeriesInstant avgDaySecondsAndResidenceSecondsInstant
                = instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(timeSeries);

        //calculate variance which is square root of square differences of distances between each instant in time series
        //and mean instant avgDaySecondsAndResidenceSecondsInstant
        long sumOfSquaredDifferences = 0;
        for (TimeSeriesInstant instant : timeSeries) {
            //calculate difference between every instant and mean instant
            long squaredDifference = instant.distanceInSecondsTo(avgDaySecondsAndResidenceSecondsInstant)
                    * instant.distanceInSecondsTo(avgDaySecondsAndResidenceSecondsInstant);
            sumOfSquaredDifferences += squaredDifference;
        } // for

        //return variance of time series
        return sumOfSquaredDifferences / (1.0 * timeSeriesInstantCount);
        //-------------------------------- NEW -------------------------------------


        //-------------------------------- OLD -------------------------------------
//        //median of the time series
//        TimeSeriesInstant medianInstant = medianInstant(timeSeries);
//
//        //for calculation median instant should be non-null
//        if(medianInstant != null)
//        {
//            //calculate variance, standard deviation and epsilon which is 10% of the standard deviation
//            long sumOfSquaredDifferences = 0;
//            for (TimeSeriesInstant instant : timeSeries)
//            {
//                //other distance metric cannot be used here, e.g. Manhattan since it is a variance calculation
//                long squaredDifference = instant.distanceInSecondsTo(medianInstant)
//                        * instant.distanceInSecondsTo(medianInstant);
//                sumOfSquaredDifferences += squaredDifference;
//            } // for
//
//
//            //return variance of time series
//            return sumOfSquaredDifferences / (1.0 * timeSeriesInstantCount);
//        } // if
//        else    // medianInstant is null, there is no variance
//        {
//            return 0;
//        } // else
        //-------------------------------- OLD -------------------------------------


//        TimeSeriesInstant averageDateInstant = instantWithAverageDate(timeSeries);
//        //for calculation average date instant should be non-null
//        if(averageDateInstant != null)
//        {
//            //calculate variance, standard deviation and epsilon which is 10% of the standard deviation
//            long sumOfSquaredDifferences = 0;
//            for (TimeSeriesInstant instant : timeSeries)
//            {
//                //other distance metric cannot be used here, e.g. Manhattan since it is a variance calculation
//                long squaredDifference = instant.distanceInSecondsTo(averageDateInstant)
//                        * instant.distanceInSecondsTo(averageDateInstant);
//                sumOfSquaredDifferences += squaredDifference;
//            } // for
//
//
//            //return variance of time series
//            return sumOfSquaredDifferences / (1.0 * timeSeriesInstantCount);
//        } // if
//        else    //no average date instant, return 0 as variance
//        {
//            return 0;
//        } // else

    } // populationVarianceOfTimeSeries


    //helper method to calculate the MAD (median absolute deviation)
    public static double medianAbsoluteDeviation(ArrayList<TimeSeriesInstant> timeSeries) {
        //median of the time series
        TimeSeriesInstant medianInstant = medianInstant(timeSeries);

        //list to hold intermediate distances between median and other instants
        List<Long> intermediates = new ArrayList<Long>();

        //for calculation median instant should be non-null
        if (medianInstant != null) {
            for (TimeSeriesInstant instant : timeSeries) {
                intermediates.add(instant.distanceInSecondsTo(medianInstant));
            } // for

            return median(intermediates.toArray(new Long[]{}));
        } // if
        else    // no median, therefore no MAD
        {
            return 0;
        } // else
    } // medianAbsoluteDeviation


    private static long median(Long[] input) {
        if (input.length == 0) {
            throw new IllegalArgumentException("to calculate median we need at least 1 element");
        }
        Arrays.sort(input);
        if (input.length % 2 == 0) {
            return (input[input.length / 2 - 1] + input[input.length / 2]) / 2;
        }
        return input[input.length / 2];
    } // median


    public static double sampleVarianceOfTimeSeries(ArrayList<TimeSeriesInstant> timeSeries) {
        //WE ASSUME THAT given timeSeries are sorted chronologically, therefore
        //do not execute the following method
        //Collections.sort(timeSeries);


        //mean (t, d) is used by non-linear time series book
        //or median of time series is tested and produces better results, but book uses means
        int timeSeriesInstantCount = timeSeries.size();

        //if instant count is 0, then no variation in the time series, return 0
        //if instant count is 1, then again variance is 0
        if (timeSeriesInstantCount == 0 || timeSeriesInstantCount == 1) return 0;

        //-------------------------------- NEW -------------------------------------
        //the mean instant, whose arrival time in day seconds and residence seconds are average of all instants in timeSeries
        TimeSeriesInstant avgDaySecondsAndResidenceSecondsInstant
                = instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(timeSeries);

        //calculate variance which is square root of square differences of distances between each instant in time series
        //and mean instant avgDaySecondsAndResidenceSecondsInstant
        long sumOfSquaredDifferences = 0;
        for (TimeSeriesInstant instant : timeSeries) {
            //other distance metric cannot be used here, e.g. Manhattan since it is a variance calculation
            long squaredDifference = instant.distanceInSecondsTo(avgDaySecondsAndResidenceSecondsInstant)
                    * instant.distanceInSecondsTo(avgDaySecondsAndResidenceSecondsInstant);
            sumOfSquaredDifferences += squaredDifference;
        } // for

        //return variance of time series
        return sumOfSquaredDifferences / (1.0 * (timeSeriesInstantCount - 1));
        //-------------------------------- NEW -------------------------------------


        //-------------------------------- OLD -------------------------------------
//        //median of the time series
//        TimeSeriesInstant medianInstant = medianInstant(timeSeries);
//
//        //for calculation median instant should be non-null
//        if(medianInstant != null)
//        {
//            //calculate variance, standard deviation and epsilon which is 10% of the standard deviation
//            long sumOfSquaredDifferences = 0;
//            for (TimeSeriesInstant instant : timeSeries)
//            {
//                //other distance metric cannot be used here, e.g. Manhattan since it is a variance calculation
//                long squaredDifference = instant.distanceInSecondsTo(medianInstant)
//                        * instant.distanceInSecondsTo(medianInstant);
//                sumOfSquaredDifferences += squaredDifference;
//            } // for
//
//
//            //return variance of time series
//            return sumOfSquaredDifferences / ( 1.0 * (timeSeriesInstantCount - 1) );
//        } // if
//        else    // medianInstant is null, there is no variance
//        {
//            return 0;
//        } // else
        //-------------------------------- OLD -------------------------------------


//        TimeSeriesInstant averageDateInstant = instantWithAverageDate(timeSeries);
//        //for calculation average date instant should be non-null
//        if(averageDateInstant != null)
//        {
//            //calculate variance, standard deviation and epsilon which is 10% of the standard deviation
//            long sumOfSquaredDifferences = 0;
//            for (TimeSeriesInstant instant : timeSeries)
//            {
//                //other distance metric cannot be used here, e.g. Manhattan since it is a variance calculation
//                long squaredDifference = instant.distanceInSecondsTo(averageDateInstant)
//                        * instant.distanceInSecondsTo(averageDateInstant);
//                sumOfSquaredDifferences += squaredDifference;
//            } // for
//
//
//            //return variance of time series
//            return sumOfSquaredDifferences / ( 1.0 * (timeSeriesInstantCount - 1) );
//        } // if
//        else    //no average date instant, return 0 as variance
//        {
//            return 0;
//        } // else

    } // sampleVarianceOfTimeSeries


    private static TimeSeriesInstant medianInstant(ArrayList<TimeSeriesInstant> timeSeries) {
        if (timeSeries == null || timeSeries.isEmpty())
            return null;

        //instead mean (t, d), or mean of time series, it is statistically optimal to find median of time series
        int timeSeriesInstantCount = timeSeries.size();

        //if there os only one element, then it is the median instant itself
        if (timeSeriesInstantCount == 1)
            return timeSeries.get(0); // return one and only instant as a median instant


        //at this point timeSeriesInstantCount >= 2, so we are safe to calculate median instant from at least two instants
        //if size or count of time series instant of time series is even
        if (timeSeriesInstantCount % 2 == 0) {
            TimeSeriesInstant instantOne = timeSeries.get(timeSeriesInstantCount / 2);
            TimeSeriesInstant instantTwo = timeSeries.get((timeSeriesInstantCount / 2) - 1);

            /*
            //since we have averageOfDates method, we can find the average above instants
            //in this case arrivalTimeInDaySeconds will be adjusted automatically,
            //but we should find average of residence seconds
            ArrayList<Date> instants = new ArrayList<Date>();
            instants.add(instantOne.getArrivalTime());
            instants.add(instantTwo.getArrivalTime());
            Date averageDate = Utils.averageOfDates(instants);

            long averageResidenceSeconds = (instantOne.getResidenceTimeInSeconds()
                    + instantTwo.getResidenceTimeInSeconds()) / 2;

            //now calculate and return median instant (median of the time series)
            return new TimeSeriesInstant(averageDate, averageResidenceSeconds);
            */


            //instead return instant with average arrival time in day seconds and average residence seconds
            ArrayList<TimeSeriesInstant> twoInstants = new ArrayList<TimeSeriesInstant>();
            twoInstants.add(instantOne);
            twoInstants.add(instantTwo);
            return instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(twoInstants);


            //choose median randomly from one of these instants
            //Random random = new Random();
            //medianInstant = (random.nextInt() % 2 == 0) ? instantOne : instantTwo;
        } // if
        else {
            //if instant count is odd, choose the element in the middle as a median instant (median of the time series)
            //we use integer division; 3/2 => 1, 5/2 => 2, 7/2 => 3
            return timeSeries.get(timeSeriesInstantCount / 2);
        } // else

    } // medianInstant


    //helper method to find the average of dates
    private static Date averageOfDates(ArrayList<Date> dates) {
        BigInteger total = BigInteger.ZERO;
        for (Date date : dates) {
            total = total.add(BigInteger.valueOf(date.getTime()));
        }
        BigInteger averageMillis = total.divide(BigInteger.valueOf(dates.size()));

        //return the average Date
        return new Date(averageMillis.longValue());
    } // averageOfDates


    //helper method to find the average of dates
    public static TimeSeriesInstant instantWithAverageDate(ArrayList<TimeSeriesInstant> instants) {
        //if the instants is null or instants is empty, there is average instant to return
        if (instants == null || instants.isEmpty()) {
            return null;
        } // if


        BigInteger totalMillis = BigInteger.ZERO;
        long sumOfResidenceSeconds = 0;

        for (TimeSeriesInstant instant : instants) {
            Date date = instant.getArrivalTime();

            //sum up each date's milliseconds
            totalMillis = totalMillis.add(BigInteger.valueOf(date.getTime()));

            //sum up residence seconds
            sumOfResidenceSeconds += instant.getResidenceTimeInSeconds();
        } // for

        BigInteger averageMillis = totalMillis.divide(BigInteger.valueOf(instants.size()));
        long avgResidenceSeconds = sumOfResidenceSeconds / instants.size();

        //return the average Date
        return new TimeSeriesInstant(new Date(averageMillis.longValue()), avgResidenceSeconds);
    } // instantWithAverageDate


    //helper method to obtain time series instant having average arrival time in seconds
    //and average residence seconds of the given time series instants
    public static TimeSeriesInstant instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(ArrayList<TimeSeriesInstant> instants) {
        if (instants == null || instants.isEmpty()) {
            return null;
        } // if


        /*
        //METHOD 1: averaging which considers chronological ordering
        //THIS METHOD IS USEFUL NOT ONLY FOR INSTANCES ON THE SAME DAY, BUT ALSO FOR INSTANCES ON DIFFERENT DAYS
        //at this point we have at least one time series instant in instants array list
        TimeSeriesInstant first = instants.get(0);
        //find the sum of distances between first and others, average them and add the average
        //to the first instant's arrival time in day seconds
        long sumOfDistances = 0;
        long sumOfResidenceSeconds = first.getResidenceTimeInSeconds(); // because index starts from 1 below

        for(int index = 1; index < instants.size(); index ++)
        {
            TimeSeriesInstant instantAtIndex = instants.get(index);
            long distance = first.distanceInSecondsTo(instantAtIndex);
            sumOfDistances += distance;

            //also sum up the residence seconds
            sumOfResidenceSeconds += instantAtIndex.getResidenceTimeInSeconds();
        } // for

        //find average arrival time in day seconds and residence seconds
        //mod 86400 is required for making arrival time in day seconds within the interval [0, 86400]
        long avgArrivalTimeInDaySeconds = (first.getArrivalTimeInDaySeconds() + sumOfDistances / instants.size()) % 86400;
        long avgResidenceSeconds = sumOfResidenceSeconds / instants.size();
        */


        //METHOD 2: straightforward averaging
        //here we just some up arrival time in day seconds and divide by instants.size() and do mod 86400
        //SHOULD BE USED ALONG WITH ABSOLUTE VALUE DISTANCE IN TimeSeriesInstant.distanceInSecondsTo() rather before() or after()
        long sumOfArrivalTimesInDaySeconds = 0;
        long sumOfResidenceSeconds = 0;
        for (int index = 0; index < instants.size(); index++) {
            sumOfArrivalTimesInDaySeconds += instants.get(index).getArrivalTimeInDaySeconds();
            sumOfResidenceSeconds += instants.get(index).getResidenceTimeInSeconds();
        } // for
        long avgArrivalTimeInDaySeconds = (sumOfArrivalTimesInDaySeconds / instants.size()) % 86400;
        long avgResidenceSeconds = sumOfResidenceSeconds / instants.size();


        //for the time series instant we also need a date part
        //it can be obtained using three ways below
        // => 1) Unix Epoch Instant -> TimeSeriesInstant.distanceInSecondsTo() should use absolute value distance,
        //rather than before() or after();
        // * 1000 is for milliseconds, because Date constructor accepts long millis
        Date avgArrivalTimeWithRespectToUnixEpoch // time zone time difference is eliminated by TimeZone.getDefault().getOffset()
                = unixEpochDateWithDaySeconds(avgArrivalTimeInDaySeconds);
        //new Date( avgArrivalTimeInDaySeconds * 1000 - TimeZone.getDefault().getOffset(0 * 1000) );
        // avgArrivalTimeInDaySeconds after 00:00, January 1st, 1970
        //now return average time series instant with avgArrivalTime and avgResidenceSeconds
        return new TimeSeriesInstant(avgArrivalTimeWithRespectToUnixEpoch, avgResidenceSeconds);



        /*
        //If Daylight Saving Time is in effect at the specified date, the offset value is adjusted with the amount of daylight saving.
        //getOffset() method receives the amount of time in milliseconds to add to UTC to get local time.
        //Here all dates are created in local time, such that
        //Date created on avgArrivalTimeInDaySeconds will add the UTC offset to it, which we do not want
        //therefore we have to subtract offset value to get time represented only by avgArrivalTimeInDaySeconds
        //long timeDifference = 4 * 3600 * 1000; => NO NEED SINCE TimeZone.getDefault().getOffset(0 * 1000 achieves the same thing
        // => 2) Average Date Instant
        // * 1000 is for milliseconds, because Date constructor accepts long millis
        Date avgDaySecondsDate
            = unixEpochDateWithDaySeconds(avgArrivalTimeInDaySeconds);
                //new Date(avgArrivalTimeInDaySeconds * 1000 - TimeZone.getDefault().getOffset(0 * 1000)); //- timeDifference);
                                            // avgArrivalTimeInDaySeconds after 00:00, January 1st, 1970

        //average date instant where arrival time will be updated using avgDaySecondsDate
        TimeSeriesInstant averageDateInstant = instantWithAverageDate(instants);

        //c is the Calendar instance for avgDaySecondsDate
        Calendar c = Calendar.getInstance();
        c.setTime(avgDaySecondsDate);

        //System.out.println(c.getTimeZone());

        //update the arrival time of averageDateInstant with hours, minutes and seconds
        Date newAvgArrivalTime = Utils.getDate(averageDateInstant.getArrivalTime(),
                c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND));

        //now return average time series instant with avgArrivalTime and avgResidenceSeconds
        return new TimeSeriesInstant(newAvgArrivalTime, avgResidenceSeconds);
        */



        /*
        // => 3) Median TimeSeriesInstant
        // * 1000 is for milliseconds, because Date constructor accepts long millis
        Date avgDaySecondsDate
                = unixEpochDateWithDaySeconds(avgArrivalTimeInDaySeconds);
                    //new Date(avgArrivalTimeInDaySeconds * 1000 - TimeZone.getDefault().getOffset(0 * 1000));
                                                    // avgArrivalTimeInDaySeconds after 00:00, January 1st, 1970

        TimeSeriesInstant medianInstant = medianInstant(instants);
        medianInstant.getArrivalTime().setHours(avgDaySecondsDate.getHours());
        medianInstant.getArrivalTime().setMinutes(avgDaySecondsDate.getMinutes());
        medianInstant.getArrivalTime().setSeconds(avgDaySecondsDate.getSeconds());

        //now return average time series instant with avgArrivalTime and avgResidenceSeconds
        return new TimeSeriesInstant(medianInstant.getArrivalTime(), avgResidenceSeconds);
        */
    } // instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds



    /*
    //method to find distance between two time series
    public static double distanceBetweenTwoTimeSeries(ArrayList<TimeSeriesInstant> firstTimeSeries,
                                                      ArrayList<TimeSeriesInstant> secondTimeSeries,
                                                      NextPlaceConfiguration npConf)
    {
        return distanceBetweenTwoTimeSeries(firstTimeSeries, secondTimeSeries, npConf.getDistanceMetric());
    } // distanceBetweenTwoTimeSeries


    //overloaded method that returns the distance by the given distance metric
    public static double distanceBetweenTwoTimeSeries(ArrayList<TimeSeriesInstant> firstTimeSeries,
                                                    ArrayList<TimeSeriesInstant> secondTimeSeries,
                                                    DistanceMetric distanceMetric)
    {
        if(firstTimeSeries == null
                || firstTimeSeries.isEmpty()
                || secondTimeSeries == null
                || secondTimeSeries.isEmpty() )
        {
            System.err.println("firstTimeSeries or secondTimeSeries is null or empty;"
                    + " no possibility to calculate distance between them - returning 0");
            return 0;
        } // if


        //report if they have different sizes
        if(firstTimeSeries.size() != secondTimeSeries.size())
            System.err.println("Size of the firstTimeSeries is different than the size of secondTimeSeries");


        //the size of time series can differ adjust the number of comparisons
        //to compare equal number of corresponding elements;
        //#comparisons will be equal to size of smaller-sized time series
        int numberOfComparisons = firstTimeSeries.size() > secondTimeSeries.size() ? secondTimeSeries.size() : firstTimeSeries.size();

        //switch distance metrics for calculating distances between two time series
        switch(distanceMetric)
        {
            case Manhattan: {

                // Manhattan distance: Take the sum of the absolute values of the differences.
                // Let s_0 = (t_0, d_0), s_1 = (t_1, d_1), s_2 = (t_2, d_2) and s_3 = (t_3, d_3)
                // where s is time series instant, t is arrival time in day seconds and d is residence time in seconds
                // and if B_1=(s_0, s_1) and B_3=(s_2, s_3), the Manhattan distance between
                // embedding vectors B_1 and B_3 is d(s_0, s_2) + d(s_1, s_3) where
                // e.g. d(s_0, s_2) is |t_0 - t_2| and d(s_1, s_3) is |t_1 - t_3| if both t are on the same day
                // otherwise 86400-based calculation will be applied if one t is on one day and other t is on the next day

                long cumulativeDistance = 0;

                for (int index = 0; index < numberOfComparisons; index++) {
                    cumulativeDistance += firstTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index));
                } // for

                return cumulativeDistance;
            } // case Manhattan


            case Euclidean: {
                // Euclidean distance: Take the square root of the sum of the squares of the differences.
                // Let s_0 = (t_0, d_0), s_1 = (t_1, d_1), s_2 = (t_2, d_2) and s_3 = (t_3, d_3)
                // where s is time series instant, t is arrival time in day seconds and d is residence time in seconds
                // and if B_1=(s_0, s_1) and B_3=(s_2, s_3), the Euclidean distance between
                // embedding vectors B_1 and B_3 is sqrt( d(s_0, s_2) * d(s_0, s_2) + d(s_1, s_3) * d(s_1, s_3) ) where
                // e.g. d(s_0, s_2) is |t_0 - t_2| and d(s_1, s_3) is |t_1 - t_3| if both t are on the same day
                // otherwise 86400-based calculation will be applied if one t is on one day and other t is on the next day


                double cumulativeSquaredDistance = 0;

                //timeSeries.size() and other.getTimeSeries().size() have the same value
                for (int index = 0; index < numberOfComparisons; index++)
                {
                    double distance = firstTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index));

                    //square the distance and sum up the squared distances
                    cumulativeSquaredDistance += distance * distance;
                } // for

                //return the sqrt of cumulative squared distance
                return Math.sqrt(cumulativeSquaredDistance);
            } // case Euclidean


            case ComparativeManhattan: {

                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------
                //Assume that B_3 = (s_0, s_1, s_2, s_3), B_10 = (s_7, s_8, s_9, s_10)
                //d(B_3, B_10) will be calculated as follows:
                //1) Compute comparative distances and store them:
                //Store d_1_0 = d(s_1, s_0), d_2_1 = d(s_2, s_1), d_3_2 = d(s_3, s_2) in one array list
                //Store d_8_7 = d(s_8, s_7), d_9_8 = (s_9, s_8), d_10_9 = d(s_10, s_9) in the other array list
                //2) Then compute d_x = d_1_0 - d_8_7, d_y = d_2_1 - d_9_8, d_z = d_3_2 - d_10_9
                //3) Compute final distance using one of the following:
                //3.1) D_manhattan = d_x + d_y + d_z
                //3.2) D_euclidean = sqrt( d_x * d_x + d_y * d_y + d_z * d_z )
                ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                for (int index = 0; index < numberOfComparisons - 1; index++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = firstTimeSeries.get(index).distanceInSecondsTo(firstTimeSeries.get(index + 1));
                    //add it to the first array list
                    thisComparativeDistances.add(thisComparativeDistance);

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = secondTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index + 1));
                    //add it to the second array list
                    otherComparativeDistances.add(otherComparativeDistance);
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------

                double cumulativeComparativeDistanceDifference = 0;

                //now find the difference between corresponding components of comparative distances
                //thisComparativeDistances and otherComparativeDistances have the same size
                for (int index = 0; index < thisComparativeDistances.size(); index ++)
                {
                    //e.g. d_x = d_1_0 - d_8_7
                    double comparativeDistanceDifference
                            = Math.abs(thisComparativeDistances.get(index) - otherComparativeDistances.get(index));

                    //sum them up
                    cumulativeComparativeDistanceDifference += comparativeDistanceDifference;
                } // for

                return cumulativeComparativeDistanceDifference;
            } // case Manhattan


            case ComparativeEuclidean: {

                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------
                //Assume that B_3 = (s_0, s_1, s_2, s_3), B_10 = (s_7, s_8, s_9, s_10)
                //d(B_3, B_10) will be calculated as follows:
                //1) Compute comparative distances and store them:
                //Store d_1_0 = d(s_1, s_0), d_2_1 = d(s_2, s_1), d_3_2 = d(s_3, s_2) in one array list
                //Store d_8_7 = d(s_8, s_7), d_9_8 = (s_9, s_8), d_10_9 = d(s_10, s_9) in the other array list
                //2) Then compute d_x = d_1_0 - d_8_7, d_y = d_2_1 - d_9_8, d_z = d_3_2 - d_10_9
                //3) Compute final distance using one of the following:
                //3.1) D_manhattan = d_x + d_y + d_z
                //3.2) D_euclidean = sqrt( d_x * d_x + d_y * d_y + d_z * d_z )
                ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                for (int index = 0; index < numberOfComparisons - 1; index++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = firstTimeSeries.get(index).distanceInSecondsTo(firstTimeSeries.get(index + 1));
                    //add it to the first array list
                    thisComparativeDistances.add(thisComparativeDistance);

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = secondTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index + 1));
                    //add it to the second array list
                    otherComparativeDistances.add(otherComparativeDistance);
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------

                double cumulativeSquaredComparativeDistanceDifference = 0;

                //now find the difference between corresponding components of comparative distances
                //thisComparativeDistances and otherComparativeDistances have the same size
                for(int index = 0; index < thisComparativeDistances.size(); index ++)
                {
                    //e.g. d_x = d_1_0 - d_8_7
                    double comparativeDistanceDifference
                            = Math.abs(thisComparativeDistances.get(index) - otherComparativeDistances.get(index));

                    //square the distance and sum the results up
                    cumulativeSquaredComparativeDistanceDifference
                            += comparativeDistanceDifference * comparativeDistanceDifference;
                } // for

                return Math.sqrt(cumulativeSquaredComparativeDistanceDifference);

            } // case ComparativeEuclidean




            case CustomLoopingManhattan: {

                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------
                //Assume that B_3 = (s_0, s_1, s_2, s_3), B_10 = (s_7, s_8, s_9, s_10)
                //d(B_3, B_10) will be calculated as follows:
                //1) Compute comparative distances and store them:
                //Store d_1_0 = d(s_1, s_0), d_2_1 = d(s_2, s_1), d_3_2 = d(s_3, s_2) in one array list
                //Store d_8_7 = d(s_8, s_7), d_9_8 = (s_9, s_8), d_10_9 = d(s_10, s_9) in the other array list
                //2) Then compute d_x = d_1_0 - d_8_7, d_y = d_2_1 - d_9_8, d_z = d_3_2 - d_10_9
                //3) Compute final distance using one of the following:
                //3.1) D_manhattan = d_x + d_y + d_z
                //3.2) D_euclidean = sqrt( d_x * d_x + d_y * d_y + d_z * d_z )
                ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                for (int index = 0; index < numberOfComparisons - 1; index++)
                {
                    //do comparative distance calculation for the first time series
                    double thisComparativeDistance = firstTimeSeries.get(index).distanceInSecondsTo(firstTimeSeries.get(index + 1));
                    //add it to the first array list
                    thisComparativeDistances.add(thisComparativeDistance);

                    //now do the the same thing for the other time series
                    double otherComparativeDistance = secondTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index + 1));
                    //add it to the second array list
                    otherComparativeDistances.add(otherComparativeDistance);
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------


                while (true)
                {
                    //thisComparativeDistances and otherComparativeDistances have the same size
                    //recursion will stop when there is only one element left in each array list
                    if (thisComparativeDistances.size() == 1) // if it is 1, then otherComparativeDistances.size() is also 1
                    {
                        //thisComparativeDistances => {d_x}; otherComparativeDistances => {d_y}
                        //Manhattan: d_x + d_y
                        return thisComparativeDistances.get(0) + otherComparativeDistances.get(0);

                        //Euclidean: sqrt( d_x * d_x + d_y * d_y )
                        //return Math.sqrt( thisComparativeDistances.get(0) * thisComparativeDistances.get(0)
                        //        + otherComparativeDistances.get(0) * otherComparativeDistances.get(0) );
                    } // if


                    //otherwise generate a new array lists of comparative distances
                    ArrayList<Double> firstList = new ArrayList<Double>();
                    ArrayList<Double> secondList = new ArrayList<Double>();
                    //if we have the following:
                    //thisComparativeDistances => {d_1, d_2, d_3}; otherComparativeDistances => (d_4, d_5, d_6)
                    //Then new reconstructed array list will contain the following:
                    //firstList => (|d_1 - d_2|, |d_2 - d_3|); secondList => {|d_4 - d_5|, |d_5 - d_6|}
                    //and this method will be recursively called with reconstructed array lists
                    for (int index = 0; index < thisComparativeDistances.size() - 1; index++) {
                        //do comparative distance calculation for the first given array list
                        double firstComparativeDistance = Math.abs(thisComparativeDistances.get(index) - thisComparativeDistances.get(index + 1));
                        //add it to the first reconstructed array list
                        firstList.add(firstComparativeDistance);

                        //now do the the same thing for the the second given array list
                        double secondComparativeDistance = Math.abs(otherComparativeDistances.get(index) - otherComparativeDistances.get(index + 1));
                        //add it to the second reconstructed array list
                        secondList.add(secondComparativeDistance);
                    } // for


                    //assign firstList and secondList accordingly
                    thisComparativeDistances = firstList;
                    otherComparativeDistances = secondList;
                } // while
            } // case CustomLoopingManhattan



            case CustomLoopingEuclidean: {

                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------
                //Assume that B_3 = (s_0, s_1, s_2, s_3), B_10 = (s_7, s_8, s_9, s_10)
                //d(B_3, B_10) will be calculated as follows:
                //1) Compute comparative distances and store them:
                //Store d_1_0 = d(s_1, s_0), d_2_1 = d(s_2, s_1), d_3_2 = d(s_3, s_2) in one array list
                //Store d_8_7 = d(s_8, s_7), d_9_8 = (s_9, s_8), d_10_9 = d(s_10, s_9) in the other array list
                //2) Then compute d_x = d_1_0 - d_8_7, d_y = d_2_1 - d_9_8, d_z = d_3_2 - d_10_9
                //3) Compute final distance using one of the following:
                //3.1) D_manhattan = d_x + d_y + d_z
                //3.2) D_euclidean = sqrt( d_x * d_x + d_y * d_y + d_z * d_z )
                ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                for (int index = 0; index < numberOfComparisons - 1; index++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = firstTimeSeries.get(index).distanceInSecondsTo(firstTimeSeries.get(index + 1));
                    //add it to the first array list
                    thisComparativeDistances.add(thisComparativeDistance);

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = secondTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index + 1));
                    //add it to the second array list
                    otherComparativeDistances.add(otherComparativeDistance);
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------


                while (true)
                {
                    //thisComparativeDistances and otherComparativeDistances have the same size
                    //recursion will stop when there is only one element left in each array list
                    if (thisComparativeDistances.size() == 1) // if it is 1, then otherComparativeDistances.size() is also 1
                    {
                        //thisComparativeDistances => {d_x}; otherComparativeDistances => {d_y}
                        //Manhattan: d_x + d_y
                        //return thisComparativeDistances.get(0) + otherComparativeDistances.get(0);

                        //Euclidean: sqrt( d_x * d_x + d_y * d_y )
                        return Math.sqrt( thisComparativeDistances.get(0) * thisComparativeDistances.get(0)
                                + otherComparativeDistances.get(0) * otherComparativeDistances.get(0) );
                    } // if


                    //otherwise generate a new array lists of comparative distances
                    ArrayList<Double> firstList = new ArrayList<Double>();
                    ArrayList<Double> secondList = new ArrayList<Double>();
                    //if we have the following:
                    //thisComparativeDistances => {d_1, d_2, d_3}; otherComparativeDistances => (d_4, d_5, d_6)
                    //Then new reconstructed array list will contain the following:
                    //firstList => (|d_1 - d_2|, |d_2 - d_3|); secondList => {|d_4 - d_5|, |d_5 - d_6|}
                    //and this method will be recursively called with reconstructed array lists
                    for (int index = 0; index < thisComparativeDistances.size() - 1; index++) {
                        //do comparative distance calculation for the first given array list
                        double firstComparativeDistance = Math.abs(thisComparativeDistances.get(index) - thisComparativeDistances.get(index + 1));
                        //add it to the first reconstructed array list
                        firstList.add(firstComparativeDistance);

                        //now do the the same thing for the the second given array list
                        double secondComparativeDistance = Math.abs(otherComparativeDistances.get(index) - otherComparativeDistances.get(index + 1));
                        //add it to the second reconstructed array list
                        secondList.add(secondComparativeDistance);
                    } // for


                    //assign firstList and secondList accordingly
                    thisComparativeDistances = firstList;
                    otherComparativeDistances = secondList;
                } // while

            } // case CustomLoopingEuclidean


            //default is manhattan
            default:
            {
                long cumulativeDistance = 0;

                for (int index = 0; index < numberOfComparisons; index++) {
                    cumulativeDistance += firstTimeSeries.get(index).distanceInSecondsTo(secondTimeSeries.get(index));
                } // for

                return cumulativeDistance;
            } // default

        } // switch
    } // distanceBetweenTwoTimeSeries
    */


    //method to sort the given tree map by values
    //in this case, new linked hash map will be returned whose values are sorted in ascending order (which is the default of Long)
    public static LinkedHashMap<Integer, Double> sortMapByValues(
            Map<Integer, Double> passedMap) {
        List<Integer> mapKeys = new ArrayList<Integer>(passedMap.keySet());
        List<Double> mapValues = new ArrayList<Double>(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);  //convenience call, tree map's keys are already sorted; Determining if a list is sorted takes at least O(n)

        LinkedHashMap<Integer, Double> sortedMap =
                new LinkedHashMap<Integer, Double>();

        Iterator<Double> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Double val = valueIt.next();
            Iterator<Integer> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                Integer key = keyIt.next();
                Double comp1 = passedMap.get(key);
                Double comp2 = val;

                if (comp1.equals(comp2)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                } // if
            } // while
        } // while

        return sortedMap;
    } // sortMapByValues


    /**
     * helper method to convert seconds from Unix epoch to 24-hour interval
     * Unix Epoch is January 1st, 1970 at UTC
     * the unix time stamp is merely the number of seconds between a particular date and the Unix Epoch
     * 24-hour interval is changing between [0, 86400] seconds
     *
     * @param unixTimeStampInSeconds Unix Epoch in seconds
     * @return day seconds between [0, 86400]
     */
    public static long toDaySeconds(long unixTimeStampInSeconds) {
        //obtain date from Unix timestamp in seconds
        Date date = new Date(unixTimeStampInSeconds * 1000);
        return toDaySeconds(date);
    } // toDaySeconds

    //get day seconds from Date
    public static long toDaySeconds(Date date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(date);

        //long daySeconds = date.getSeconds() + date.getMinutes() * 60 + date.getHours() * 3600;

        //Calendar.HOUR is strictly for 12 hours => HOUR is used for the 12-hour clock (0 - 11).
        //Calender.HOUR_OF_DAY => HOUR_OF_DAY is used for the 24-hour clock.

        long daySeconds = (c.get(Calendar.SECOND) +
                c.get(Calendar.MINUTE) * 60 +
                c.get(Calendar.HOUR_OF_DAY) * 3600);

        return daySeconds;
    } // toDaySeconds


    //helper method to calculate number of days between dates
    public static long noOfDaysBetween(Date firstDate, Date secondDate)
    {
        if (firstDate.before(secondDate))
            return (secondDate.getTime() - firstDate.getTime())
                    / (1000 * 60 * 60 * 24);
        else if (secondDate.before(firstDate))
            return (firstDate.getTime() - secondDate.getTime())
                    / (1000 * 60 * 60 * 24);
        else
            return 0;
    } // noOfDaysBetween


    public static Date toDate(String dateString, String dateFormatPattern) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(dateFormatPattern, Locale.ENGLISH);
        return dateFormat.parse(dateString);
    } // toDate


    //toDateStringWithoutTime is used to remove time from the date
    public static String toDateStringWithoutTime(Date date) {
        return date.toString().replaceAll("(\\d\\d:){2}\\d\\d\\s", "");
    } // toDateStringWithoutTime


    //date string without day month year
    public static String toDateStringWithoutDayMonthYear(Date date) {
        SimpleDateFormat sdf =
                new SimpleDateFormat("HH:mm:ss zzz"); //("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
        return sdf.format(date);
    } // toDateStringWithoutDayMonthYear


    //helper method to find minimum latitude of the given locations
    public static Tuple2<Double, Double> minLatitudeLongitudeTuple(List<Location> locations)
    {
        //The valid range of latitude in degrees is >> -90 and +90 << for the southern and northern hemisphere respectively.
        //Longitude is in the range >> -180 and +180 << specifying coordinates west and east of the Prime Meridian, respectively.
        double minLat = 90; // by default, which will be a lot smaller when we loop through all locations
        double minLng = 180; // by default, which will be a lot smaller when we loop through all locations
        for (Location location : locations) {
            double thisLatitude = location.getLatitude();
            double thisLongitude = location.getLongitude();

            //find min latitude and min longitude
            if (thisLatitude < minLat)
                minLat = thisLatitude;

            if (thisLongitude < minLng)
                minLng = thisLongitude;
        } // for

        return new Tuple2<Double, Double>(minLat, minLng);
    } // minLatitudeLongitudeTuple


    //helper  method to find max latitude and longitude tuple
    public static Tuple2<Double, Double> maxLatitudeLongitudeTuple(List<Location> locations) {
        //The valid range of latitude in degrees is >> -90 and +90 << for the southern and northern hemisphere respectively.
        //Longitude is in the range >> -180 and +180 << specifying coordinates west and east of the Prime Meridian, respectively.
        double maxLat = -90; // by default, which will be a lot bigger when we loop through all locations
        double maxLng = -180; // by default, which will be a lot bigger when we loop through all locations
        for (Location location : locations) {
            double thisLatitude = location.getLatitude();
            double thisLongitude = location.getLongitude();

            //find min latitude and min longitude
            if (thisLatitude > maxLat)
                maxLat = thisLatitude;

            if (thisLongitude > maxLng)
                maxLng = thisLongitude;
        } // for

        return new Tuple2<Double, Double>(maxLat, maxLng);
    } // maxLatitudeLongitudeTuple


    //returns min latitude and longitude tuple from the list of visits
    public static Tuple2<Double, Double> minLatitudeLongitudeTuple(ArrayList<Visit> visits) {
        //The valid range of latitude in degrees is >> -90 and +90 << for the southern and northern hemisphere respectively.
        //Longitude is in the range >> -180 and +180 << specifying coordinates west and east of the Prime Meridian, respectively.
        double minLat = 90; // by default, which will be a lot smaller when we loop through all locations
        double minLng = 180; // by default, which will be a lot smaller when we loop through all locations
        for (Visit v : visits) {
            Location location = v.getLocation();
            double thisLatitude = location.getLatitude();
            double thisLongitude = location.getLongitude();

            //find min latitude and min longitude
            if (thisLatitude < minLat)
                minLat = thisLatitude;

            if (thisLongitude < minLng)
                minLng = thisLongitude;
        } // for

        return new Tuple2<Double, Double>(minLat, minLng);
    } // minLatitudeLongitudeTuple


    //helper  method to find max latitude and longitude tuple
    public static Tuple2<Double, Double> maxLatitudeLongitudeTuple(ArrayList<Visit> visits) {
        //The valid range of latitude in degrees is >> -90 and +90 << for the southern and northern hemisphere respectively.
        //Longitude is in the range >> -180 and +180 << specifying coordinates west and east of the Prime Meridian, respectively.
        double maxLat = -90; // by default, which will be a lot bigger when we loop through all locations
        double maxLng = -180; // by default, which will be a lot bigger when we loop through all locations
        for (Visit v : visits) {
            Location location = v.getLocation();
            double thisLatitude = location.getLatitude();
            double thisLongitude = location.getLongitude();

            //find min latitude and min longitude
            if (thisLatitude > maxLat)
                maxLat = thisLatitude;

            if (thisLongitude > maxLng)
                maxLng = thisLongitude;
        } // for

        return new Tuple2<Double, Double>(maxLat, maxLng);
    } // maxLatitudeLongitudeTuple


    //helper method to calculate the 2D gaussian of the given muLocation
    public static double gaussian(Location muLocation, double lat, double lng, double sigma, boolean isCoordinateLevel) {
        return gaussian(muLocation, new Location(lat, lng), sigma, isCoordinateLevel);
    } // gaussian


    //gaussian (probability density function) value between this muLocation (mean) and other locations
    public static double gaussian(Location muLocation, Location other, double sigma, boolean isCoordinateLevel)
    {
        //In two dimensions, the circular Gaussian function is the distribution function for uncorrelated variates
        //X and Y having a bivariate normal distribution and equal standard deviation sigma = sigma_x = sigma_y,
        //f(x, y) = 1 / (2 * pi * sigma^2) * e^( -[ (x - mu_x)^2 + (y - mu_y)^2 ] / (2 * sigma^2) ).

        //sigma = 10 (meters) (as proposed on the paper) which is related to average GPS accuracy
        if (isCoordinateLevel) {
            double mu_x = muLocation.getLatitude();
            double mu_y = muLocation.getLongitude();
            double other_x = other.getLatitude();
            double other_y = other.getLongitude();

            //return coordinate level gaussian value
            return (1 / (2 * Math.PI * sigma * sigma))
                    * Math.exp(-(Math.pow((other_x - mu_x), 2) + Math.pow((other_y - mu_y), 2))
                    / (2 * sigma * sigma));
        } // if
        else // else return gaussian value based on Earth distance of locations
        {
            double distance = muLocation.distanceTo(other);
            return (1 / (2 * Math.PI * sigma * sigma))
                    * Math.exp(-(distance * distance)
                    / (2 * sigma * sigma));
        } // else
    } // gaussian


    //overloaded method without isCoordinateLevel parameter
    public static double gaussian(Location muLocation, Location other, double sigma) {
        double distance = muLocation.distanceTo(other);
        return (1 / (2 * Math.PI * sigma * sigma))
                * Math.exp(-(distance * distance)
                / (2 * sigma * sigma));
    } // gaussian


    //overloaded method with lat and lng and without isCoordinateLevel parameter
    public static double gaussian(Location muLocation, double lat, double lng, double sigma)
    {
        return gaussian(muLocation, new Location(lat, lng), sigma);
    } // gaussian


    //kernel density estimate of the given location with respect to other locations
    //returns gaussian kernel density estimates of thisLocation
    public static double standardGaussianKDE(Location thisLocation, List<Location> otherLocations, double sigma, boolean isCoordinateLevel) {
        //here every location has the same weight which is 1 / (number of locations)
        //compute pdf (gaussian) of thisLocation and other location
        double sumOfKernels = 0;
        for (Location otherLocation : otherLocations) {
            //kernel function is gaussian
            double kernelBetweenThisAndOther = gaussian(thisLocation, otherLocation, sigma, isCoordinateLevel);
            sumOfKernels += kernelBetweenThisAndOther;
        } // for

        //here weight for each location in otherLocations is 1 / otherLocations.size() as in the standard gaussian kde
        //density is the sumOfKernels divided number of locations
        return sumOfKernels / otherLocations.size();
    } // standardGaussianKDE


    //weighted gaussian kernel density estimate of thisLocation
    public static double weightedGaussianKDE(Location thisLocation, List<Location> otherLocations,
                                             List<Double> weights, double sigma) {
        //weighted kernel density estimation formula:
        //for all i = 1, ..., n, sum up W(x_i) * K((x - x_i) / sigma) where K is bivariate Kernel (in our case gaussian) function
        //and W(x_i) is the weight associated to x_i; for all i = 1, ..., n => sum of W(x_i) should be 1
        //in our case x = (x1, x2) and therefore K(x) = ( 1 / 2 * PI ) * e^( -||x||^2 );
        //x_i = (x1_i, x2_i), therefore K((x - x_i) / sigma) = ( 1 / 2 * PI ) * e^( -||x - x_i||^2 / ( 2 * sigma^2 ) );
        double sumOfKernels = 0;
        //number of otherLocations and weights should the same
        for (int index = 0; index < otherLocations.size(); index++) {
            //kernel function is gaussian
            double kernelBetweenThisAndOther = gaussian(thisLocation, otherLocations.get(index), sigma);
            sumOfKernels += weights.get(index) * kernelBetweenThisAndOther;
        } // for

        return sumOfKernels; // / otherLocations.size(); // 1 / |Data|
    } // weightedGaussianKDE


    //helper method to find the peek of the gaussian
    public static double peekOfGaussian(Location muLocation, double minLatitude, double minLongitude,
                                        double maxLatitude, double maxLongitude, double sigma) {
        //initially 0
        double peek = 0;

        /*
        for(double lat = minLatitude; lat <= maxLatitude; lat += 0.001) //0.00001) //0.0001 took 5 hours
        {
            for(double lng = minLongitude; lng <= maxLongitude; lng += 0.001) //0.00001) //0.0001 took 5 hours
            {
                double gaussian = gaussian(muLocation, lat, lng, sigma);
                if(gaussian > peek)
                    peek = gaussian;
            } // for
        } // for
        */


        ///*
        // You can find the value of the Gaussian PDF at the peak by plugging into the Gaussian density:
        // f(x)=12πσ2√e−(x−μ)2/(2σ2) to see that the peak value of the Gaussian pdf (which occurs at x=μ)
        // is 12πσ2√.
        //Check whether muLocation is latitude longitude is between minLatitude, minLongitude and maxLatitude and maxLongitude
        if (muLocation.getLatitude() <= maxLatitude && muLocation.getLatitude() >= minLatitude
                && muLocation.getLongitude() <= maxLongitude && muLocation.getLongitude() >= minLongitude) {
            //peek = gaussian(muLocation, muLocation.getLatitude(), muLocation.getLongitude(), sigma);
            peek = 1 / (2 * Math.PI * sigma * sigma);
        } // if
        //*/

        //System.err.println("PEEK: " + peek);

        return peek;
    } // peekOfGaussian


    //helper method to get the configured file system from hadoop configuration
    //if spark is in local mode, then it will get file:// which means local file system;
    //otherwise it will get from HDFS e.g. => hdfs://
    public static String getConfiguredFileSystem(Configuration hadoopConfiguration) {
        String FS_PARAM_NAME = "fs.defaultFS";
        return hadoopConfiguration.get(FS_PARAM_NAME);
    } // getConfiguredFileSystem


    //helper method to find asymmetric set difference of two sets
    public static <T> TreeSet<T> asymmetricDifference(TreeSet<T> treeSetA, TreeSet<T> treeSetB) {
        TreeSet<T> tempTreeSet = new TreeSet<T>(treeSetA);
        //s1.removeAll(s2) — transforms s1 into the (asymmetric) set difference of s1 and s2.
        //(For example, the set difference of s1 minus s2 is the set containing all of the elements found in s1 but not in s2.)
        tempTreeSet.removeAll(treeSetB);
        return tempTreeSet;
    } // asymmetricDifference


    //overloaded hash set version for asymmetric difference
    public static <T> HashSet<T> asymmetricDifference(HashSet<T> hashSetA, HashSet<T> hashSetB) {
        HashSet<T> tempHashSet = new HashSet<>(hashSetA);
        tempHashSet.removeAll(hashSetB);
        return tempHashSet;
    } // asymmetricDifference


    //fromMapToListTuple2() generic method to convert Map<T1, T2> to List<Tuple2<T1, T2>>
    public static <T1, T2> List<Tuple2<T1, T2>> fromMapToListTuple2(Map<T1, T2> map) {
        //list of tuples
        List<Tuple2<T1, T2>> list = new ArrayList<Tuple2<T1, T2>>();

        //loop through all key-value pairs add them to the list
        for (T1 key : map.keySet()) {
            //get the value
            T2 value = map.get(key);

            //Tuple2 is not a collection, but a single tuple => single pair;
            //interesting part of that is the generic type argument;
            //which is actually making it look like a collection, but in reality it is not
            Tuple2<T1, T2> tuple2 = new Tuple2<T1, T2>(key, value);

            //populate the list with created tupple2
            list.add(tuple2);
        } // for

        return list;
    } // fromMapToListTuple2


    //helper method to generate average location of the given locations
    public static Location averageLocation(Collection<Location> locations)
    {
        double sumOfLatitudes = 0;
        double sumOfLongitudes = 0;
        String locName = "UNK"; // in our two GPS datasets, locations have the name of UNk

        //go through each location and update the sum
        for (Location loc : locations) {
            sumOfLatitudes += loc.getLatitude();
            sumOfLongitudes += loc.getLongitude();
        } // for

        double averageLat = sumOfLatitudes / locations.size();
        double averageLng = sumOfLongitudes / locations.size();
        return new Location(locName, averageLat, averageLng);
    } // averageLocation


    //helper method to read file in hdfs "filePath" of hadoop distributed file system;
    //method returns the contents of the file as a string;
    //that string can be passed to java.util.Scanner, or String.split("\n") can be used to get lines of the file
    public static String fileContentsFromHDFSFilePath(Configuration hadoopConfiguration,
                                                      String hdfsFilePathString,
                                                      boolean readWithBufferedReader) throws IOException {
        if (readWithBufferedReader)
            return fileContentsFromHDFSFilePathWithBufferedReader(hadoopConfiguration, hdfsFilePathString);


        //ELSE the below statements will run
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(hdfsFilePathString);
        FileSystem fs = path.getFileSystem(hadoopConfiguration);
        FSDataInputStream inputStream = fs.open(path);

        //BufferedReader with InputStreamReader has almost the same performance as this method

        // NEW
        StringWriter sw = new StringWriter();
        BufferedWriter bw = new BufferedWriter(sw);
        IOUtils.copy(inputStream, bw, "UTF-8");
        // forces out the characters to string writer
        bw.flush();
        // string buffer is created
        StringBuffer sb = sw.getBuffer();
        fs.close();
        inputStream.close();
        sw.close();
        bw.close();
        return sb.toString();    //String is too big, OutOfMemoryError
        // NEW


        // OLD
//        StringWriter writer = new StringWriter();
//
//        //IOUtils.copy(inputStream, writer, "UTF-8"); //used BufferedWriter above, still OutOfMemoryError
//
//        fs.close(); //no more filesystem operations are needed.
//        inputStream.close(); //close input stream and release any system resources associated with the stream.
//
//        return writer.toString();
        // OLD
    } // fileContentsFromHDFSFilePath


    //helper method to write the specific content to the file <hdfsFilePathString + resultFileNameAppendix> in the directory
    //called resultFileNameAppendix which will be a subdirectory of the parent of the file at hdfsFilePathString
    public static void writeToHDFS(Configuration hadoopConfiguration, String hdfsFilePathString,
                                   String resultFileNameAppendix, String fileID, String newFileExtension, String writableContent) {
        //file system to be used in try block and to be closed in finally block
        FileSystem fs = null;

        try {
            //get the org.apache.hadoop.fs.Path from pp user file path
            Path filePath = new Path(hdfsFilePathString);
            fs = filePath.getFileSystem(hadoopConfiguration);

            //get the fully-qualified path
            filePath = fs.resolvePath(filePath);

            //if the path is a directory, print message to standard error and return
            if (fs.isDirectory(filePath)) {
                System.err.println(hdfsFilePathString + " is a directory; not able to generate writable file path from it");
                return;
            } // if

            //get the parent directory of ppUserFilePath
            Path filePathParentDir = filePath.getParent();

            //make a new directory called <resultFileNameAppendix> in the parent directory; if that directory exists do not create it
            //Path.toString() returns path string without trailing directory separator
            Path resultingSubDir = new Path(filePathParentDir.toString() + "/" + resultFileNameAppendix);

            //mkdir a directory if the constructed path does not exist
            if (!fs.exists(resultingSubDir))
                fs.mkdirs(resultingSubDir);

            //now create a writable path from resultingSubDir and the given filePath
            //Path.toString() returns path string without trailing directory separator
            String writablePathString = newFilePathWithNameAppendix(resultingSubDir.toString() + "/",
                    filePath.toString(),
                    resultFileNameAppendix + fileID, newFileExtension);

            //create a writable path to write writableContent
            Path writableFilePath = new Path(writablePathString);
            //fs.createNewFile(writableFilePath);   => no need for file creating fs.create(writableFilePath) does it for us

            //if the corresponding file exists delete that first
            if (fs.exists(writableFilePath)) {
                //we do not need a recursive deletion, because we just a delete a file
                fs.delete(writableFilePath, false);
            } // if

            //create file and write content to file; if writableFilePath does not contain a file fs.create() will create it
            FSDataOutputStream fos = fs.create(writableFilePath);

            //WRITING METHOD 1
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            br.write(writableContent);
            br.close();     // => Closes the stream, flushing it first. Once the stream has been closed,
            // => further write() or flush() invocations will cause an IOException to be thrown.
            // => Closing a previously closed stream has no effect.

            //WRITING METHOD 2
            //fos.writeChars(writableContent);
            //fos.flush();
            //fos.close();
        } // try
        catch (Exception ex) {
            ex.printStackTrace();
        } // catch
        finally {
            try {
                if (fs != null)
                    fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } // catch
        } // finally

    } // writeToHDFS


    //helper method to write the specific content to the file <localFilePathString + resultFileNameAppendix> in the directory
    //called resultFileNameAppendix which will be a subdirectory of the parent of the file at hdfsFilePathString
    public static void writeToLocalFileSystem(String localFilePathString, String resultFileNameAppendix, String fileID,
                                              String newFileExtension, String writableContent) {
        //to be closed in finally
        PrintWriter printWriter = null;

        try {
            File localFile = new File(localFilePathString);

            //if the localFile is a directory, print message to standard error and return
            if (localFile.isDirectory()) {
                System.err.println("\n" + localFilePathString + " is a directory; not able to generate writable file path from it");
            } // if
            //there is a possibility given path does not exist which means does not refer to either file or directory
            //so perform checks for each of them; which is equivalent to FileSystem.resolvePath() for HDFS
            else if (localFile.isFile()) {
                //get the parent directory of localFile
                File localFileParentDir = localFile.getParentFile();

                //make a new directory called <resultFileNameAppendix> in the parent directory; if that directory exists do not create it
                //getAbsolutePath() returns path string without trailing directory separator
                File resultingSubDir = new File(localFileParentDir.getAbsolutePath() + "/" + resultFileNameAppendix);

                //mkdir a directory if the resulting sub dir path does not exist
                if (!resultingSubDir.exists())
                    resultingSubDir.mkdir();

                //now create a writable path from resultingSubDir and the given localFile
                //getAbsolutePath() returns path string without trailing directory separator
                String writableFilePathString = newFilePathWithNameAppendix(resultingSubDir.getAbsolutePath() + "/",
                        localFile.getAbsolutePath(), resultFileNameAppendix + fileID, newFileExtension);

                //create a file from writable path
                File writableFile = new File(writableFilePathString);
                writableFile.createNewFile();

                //write with print writer
                printWriter = new PrintWriter(writableFile);
                printWriter.print(writableContent);
            } // else if
            else
                System.err.println("\n" + localFilePathString + " is neither a file nor a directory; please provide correct file path");

        } // try
        catch (Exception ex) {
            ex.printStackTrace();
        } // catch
        finally {
            if (printWriter != null) printWriter.close();
        } // finally

    } // writeToLocalFileSystem


    //helper method to copy the files from local to HDFS
    public static void copyFromLocalToHDFS(Configuration hadoopConfiguration, String localDirPathString,
                                           String hdfsDirPathString, boolean recursivelyCopyFromLocalDir) {
        //file system instance to help copy local files to HDFS; will be closed in finally block
        FileSystem fs = null;

        try {
            Path hdfsDir = new Path(hdfsDirPathString);
            fs = hdfsDir.getFileSystem(hadoopConfiguration);

            //get the fully-qualified path
            hdfsDir = fs.resolvePath(hdfsDir);

            //if hdfsDir is not actually a directory, then obtain its parent as hdfsDir
            if (!fs.isDirectory(hdfsDir))
                hdfsDir = hdfsDir.getParent();


            //if recursive copying means all sub directories and files of that subdirectories will be copied
            if (recursivelyCopyFromLocalDir) {
                //convert local dir path string to hadoop path
                Path localDir = new Path(localDirPathString);
                //get the fully-qualified path
                localDir = fs.resolvePath(localDir);

                //(heap issues with recursive approach) => using a queue
                Queue<Path> fileQueue = new LinkedList<Path>();

                //add the obtained path to the queue
                fileQueue.add(localDir);


                //while the fileQueue is not empty
                while (!fileQueue.isEmpty()) {
                    //get the file path from queue
                    Path filePath = fileQueue.remove();

                    //filePath refers to a file
                    if (fs.isFile(filePath)) {
                        //localDir and hdfsDir are fully-qualified paths and we are safe;
                        //there is a possibility that localDir can refer to a file
                        String localDirName;
                        String localDirPathStr;

                        //if localDir is a file, takes its parent for path evaluation below
                        if (fs.isFile(localDir)) {
                            localDirName = localDir.getParent().getName();
                            localDirPathStr = localDir.getParent().toString();
                        } // if
                        else    //localDir is a directory; act normally
                        {
                            localDirName = localDir.getName();                      // .idea
                            localDirPathStr = localDir.toString();                  // /../.idea
                        } // else

                        String fileParentPathString = filePath.getParent().toString(); // /../.idea/dictionaries
                        String parentDirRelativePathName = fileParentPathString.replace(localDirPathStr, "");
                        // => /dictionaries

                        //create a the same directory in HDFS as the parent of local file
                        Path resultingSubDir = new Path(hdfsDir.toString() + "/" + localDirName + parentDirRelativePathName);

                        //mkdir a directory if the constructed path does not exist
                        if (!fs.exists(resultingSubDir))
                            fs.mkdirs(resultingSubDir);

                        String fileName = fileNameFromPath(filePath.toString());
                        String currentFileExtension = "." + FilenameUtils.getExtension(fileName);

                        //get a writable file path to hdfs
                        //we won't have appendix, since we copy from local to hdfs as a plain file
                        String writableHDFSFilePath
                                = newFilePathWithNameAppendix(resultingSubDir.toString() + "/",
                                filePath.toString(), "",
                                currentFileExtension);


                        System.out.println("Copying " + filePath + " to HDFS .....");


                        //copy from local file path to hdfs writable file path
                        fs.copyFromLocalFile(filePath, new Path(writableHDFSFilePath));
                    } else   //else filePath refers to a directory
                    {
                        //list paths in the directory and add to the queue
                        FileStatus[] fileStatuses = fs.listStatus(filePath);
                        for (FileStatus fileStatus : fileStatuses) {
                            fileQueue.add(fileStatus.getPath());
                        } // for
                    } // else

                } // while

            } // if
            else    // do not recursively copy content of localDir to HDFS
            {
                //list all files from localDirPathString
                List<String> filePaths = listFilesFromLocalPath(localDirPathString, false);

                //iterate over file obtained file paths and write to hdfs
                for (String filePath : filePaths) {
                    Path localFilePath = new Path(filePath);
                    //get the parent dir name of the local file and create the same directory in HDFS and write there
                    String localFileParentDirName = localFilePath.getParent().getName();

                    //create a the same directory in HDFS as the parent of local file
                    Path resultingSubDir = new Path(hdfsDir.toString() + "/" + localFileParentDirName);

                    //mkdir a directory if the constructed path does not exist
                    if (!fs.exists(resultingSubDir))
                        fs.mkdirs(resultingSubDir);


                    String fileName = fileNameFromPath(localFilePath.toString());
                    String currentFileExtension = "." + FilenameUtils.getExtension(fileName);


                    //get a writable file path to hdfs
                    //we won't have appendix, since we copy from local to hdfs as a plain file
                    String writableHDFSFilePath = newFilePathWithNameAppendix(resultingSubDir.toString() + "/",
                            localFilePath.toString(), "", currentFileExtension);


                    System.out.println("Copying " + filePath + " to HDFS .....");


                    //copy from local file path to hdfs writable file path
                    fs.copyFromLocalFile(localFilePath, new Path(writableHDFSFilePath));
                } // for

            } // else

        } // try
        catch (Exception ex) {
            ex.printStackTrace();
        } // catch
        finally {
            try {
                if (fs != null) fs.close();
            } // try
            catch (IOException ioex) {
                ioex.printStackTrace();
            } // catch
        } // finally

    } // copyFromLocalToHDFS


    //helper method to copy the files from HDFS to local
    public static void copyFromHDFSToLocal(Configuration hadoopConfiguration, String hdfsDirPathString,
                                           String localDirPathString, boolean recursivelyCopyFromHDFSDir) {
        //file system instance to help copy local files to HDFS; will be closed in finally block
        FileSystem fs = null;

        try {
            Path localDir = new Path(localDirPathString);
            fs = localDir.getFileSystem(hadoopConfiguration);

            //get the fully-qualified path
            localDir = fs.resolvePath(localDir);

            //if localDir is not actually a directory, then obtain its parent as localDir
            if (!fs.isDirectory(localDir))
                localDir = localDir.getParent();


            //if recursive copying means all sub directories and files of that subdirectories will be copied
            if (recursivelyCopyFromHDFSDir) {
                //convert hdfs dir path string to hadoop path
                Path hdfsDir = new Path(hdfsDirPathString);
                //get the fully-qualified path
                hdfsDir = fs.resolvePath(hdfsDir);

                //(heap issues with recursive approach) => using a queue
                Queue<Path> fileQueue = new LinkedList<Path>();

                //add the obtained path to the queue
                fileQueue.add(hdfsDir);


                //while the fileQueue is not empty
                while (!fileQueue.isEmpty()) {
                    //get the file path from queue
                    Path hdfsFilePath = fileQueue.remove();

                    //hdfsFilePath refers to a file
                    if (fs.isFile(hdfsFilePath)) {
                        //localDir and hdfsDir are fully-qualified paths and we are safe;
                        //there is a possibility that hdfsDir can refer to a file
                        String hdfsDirName;
                        String hdfsDirPathStr;

                        //if hdfsDir is a file, takes its parent for path evaluation below
                        if (fs.isFile(hdfsDir)) {
                            hdfsDirName = hdfsDir.getParent().getName();
                            hdfsDirPathStr = hdfsDir.getParent().toString();
                        } // if
                        else    //hdfsDir is a directory; act normally
                        {
                            hdfsDirName = hdfsDir.getName();                      // .idea
                            hdfsDirPathStr = hdfsDir.toString();                  // /../.idea
                        } // else

                        String hdfsFileParentPathString = hdfsFilePath.getParent().toString(); // /../.idea/dictionaries
                        String parentDirRelativePathName = hdfsFileParentPathString.replace(hdfsDirPathStr, "");
                        // => /dictionaries

                        //create a the same directory in HDFS as the parent of local file
                        Path resultingSubDir = new Path(localDir.toString() + "/" + hdfsDirName + parentDirRelativePathName);

                        //mkdir a directory if the constructed path does not exist
                        if (!fs.exists(resultingSubDir))
                            fs.mkdirs(resultingSubDir);

                        String fileName = fileNameFromPath(hdfsFilePath.toString());
                        String currentFileExtension = "." + FilenameUtils.getExtension(fileName);

                        //get a writable file path to hdfs
                        //we won't have appendix, since we copy from local to hdfs as a plain file
                        String writableLocalFilePath
                                = newFilePathWithNameAppendix(resultingSubDir.toString() + "/",
                                hdfsFilePath.toString(), "", currentFileExtension);


                        System.out.println("Copying " + hdfsFilePath + " to local file system .....");


                        //copy to local writable file path from hdfs file path
                        fs.copyToLocalFile(hdfsFilePath, new Path(writableLocalFilePath));
                    } else   //else hdfsFilePath refers to a directory
                    {
                        //list paths in the directory and add to the queue
                        FileStatus[] fileStatuses = fs.listStatus(hdfsFilePath);
                        for (FileStatus fileStatus : fileStatuses) {
                            fileQueue.add(fileStatus.getPath());
                        } // for
                    } // else

                } // while

            } // if
            else    // do not recursively copy content of localDir to HDFS
            {
                //list all files from hdfsDirPathString
                List<String> hdfsFilePathStrings = listFilesFromHDFSPath(hadoopConfiguration, hdfsDirPathString, false);

                //iterate over file obtained file paths and write to hdfs
                for (String hdfsFilePathString : hdfsFilePathStrings) {
                    //listFilesFromHDFSPath() returns resolved path strings; no need to call resolvePath once again for each hdfsFilePath
                    Path hdfsFilePath = new Path(hdfsFilePathString);
                    //get the parent dir name of the local file and create the same directory in HDFS and write there
                    String hdfsFileParentDirName = hdfsFilePath.getParent().getName();

                    //create a the same directory in local file system as the parent of hdfs file
                    Path resultingSubDir = new Path(localDir.toString() + "/" + hdfsFileParentDirName);

                    //mkdir a directory if the constructed path does not exist
                    if (!fs.exists(resultingSubDir))
                        fs.mkdirs(resultingSubDir);

                    String fileName = fileNameFromPath(hdfsFilePath.toString());
                    String currentFileExtension = "." + FilenameUtils.getExtension(fileName);

                    //get a writable file path to local file system
                    //we won't have appendix, since we copy from hdfs to local as a plain file
                    String writableLocalFilePath = newFilePathWithNameAppendix(resultingSubDir.toString() + "/",
                            hdfsFilePath.toString(), "", currentFileExtension);


                    System.out.println("Copying " + hdfsFilePath + " to local system .....");


                    //copy from hdfs file path to local writable file path
                    fs.copyToLocalFile(hdfsFilePath, new Path(writableLocalFilePath));
                } // for

            } // else

        } // try
        catch (Exception ex) {
            ex.printStackTrace();
        } // catch
        finally {
            try {
                if (fs != null) fs.close();
            } // try
            catch (IOException ioex) {
                ioex.printStackTrace();
            } // catch
        } // finally

    } // copyFromLocalToHDFS


    //-----------------------------------------------------------------------------------
    // USER SHOULD PROVIDE DIR PATH TO THIS METHOD WHICH HAS TRAILING DIRECTORY SEPARATOR
    //-----------------------------------------------------------------------------------
    //helper method to generate writable file path of the file path that will be a child of given dir path;
    //writable file path will be created based on the name of the file in filePath + appendix
    //if absolute dir path is given, then newFilePathWithNameAppendix will also be absolute;
    //else if dir path is relative, then we will get relative writable path
    private static String newFilePathWithNameAppendix(String dirPathToWrite, String filePath,
                                                      String fileNameAppendix,
                                                      String newFileExtension) {
        String fileName = fileNameFromPath(filePath);
        String currentFileExtension = "." + FilenameUtils.getExtension(fileName);

        String writableFileName = fileName.replace(currentFileExtension, "")
                + /*"_NextPlace_Results"*/ fileNameAppendix + newFileExtension;


        //getFullPath() => gets the full path from a full filename, which is the prefix + path,
        //and also including the final directory separator.
        //this will ensure that dirPathToWrite will have trailing directory separator at the end of the path;
        //Examples:
        /*
        C:\a\b\c.txt --> C:\a\b\
        ~/a/b/c.txt  --> ~/a/b/
        a.txt        --> ""
        a/b/c        --> a/b/
        a/b/c/       --> a/b/c/
        C:           --> C:
        C:\          --> C:\
        ~            --> ~/
        ~/           --> ~/
        ~user        --> ~user/
        ~user/       --> ~user/
        */


        //as you from above, if the dirPath refers to file such as "a.txt", we ill get "" and as a result writableFileName will be returned
        //if dirPathToWrite refers to file such as "C:\a\b\c.txt", it will return "C:\a\b\" + writableFileName;
        //if the fileName is "", then writableFileName will be fileNameAppendix + currentFileExtension => fileNameAppendix + "";

        //the extreme case is when dirPath is a/b/c where full path is a/b/ despite of the fact that c is a directory
        //so user should provide dir path to this method which has trailing directory separator;
        return FilenameUtils.getFullPath(dirPathToWrite) + writableFileName;
    } // newFilePathWithNameAppendix


    //helper method to convert pp user file path (which resides in local file system) to pp user file contents
    public static String fileContentsFromLocalFilePath(String localFilePath) {
        InputStream localFileInputStream = inputStreamFromLocalFilePath(localFilePath);
        return stringContentFromInputStream(localFileInputStream);
    } // fileContentsFromLocalFilePath


    //helper method to read file in hdfs "filePath" of hadoop distributed file system with buffered reader
    //if spark is in local mode, then it will get file contents from local file system;
    //otherwise it will get from HDFS;
    private static String fileContentsFromHDFSFilePathWithBufferedReader(Configuration hadoopConfiguration, String hdfsFilePath) {
        FileSystem fs = null;
        FSDataInputStream inputStream = null;

        try {
            Path path = new Path(hdfsFilePath);
            fs = path.getFileSystem(hadoopConfiguration);
            inputStream = fs.open(path);

            //instead of reading whole file input stream at once, we read the file input stream character by character
            //in stringContentFromInputStream() method;
            //we create a buffered reader from input stream via input stream reader there;
            return stringContentFromInputStream(inputStream);
        } // try
        catch (Exception ex) // will catch all exceptions including "IOException"
        {
            ex.printStackTrace();

            //if any problem occurs during read operation, return an empty string which will yield empty array list of visits
            return "";
        } // catch
        finally {
            //close file system
            if (fs != null) {
                try {
                    fs.close(); //no more filesystem operations are needed.
                } catch (IOException e) {
                    e.printStackTrace();
                } // catch
            } // if

            //close input stream
            if (inputStream != null) {
                try {
                    inputStream.close(); //close input stream and release any system resources associated with the stream.
                } catch (IOException e) {
                    e.printStackTrace();
                } // catch
            } // if

        } // finally

    } // fileContentsFromHDFSFilePathWithBufferedReader


    //helper method to get the list of files from the HDFS path
    public static List<String> listFilesFromHDFSPath(Configuration hadoopConfiguration, String hdfsPathString,
                                                     boolean recursive)
    {
        //resulting list of files
        List<String> filePaths = new ArrayList<String>();
        FileSystem fs = null;

        //try-catch-finally all possible exceptions
        try {
            //get path from string and then the filesystem
            Path path = new Path(hdfsPathString);  //throws IllegalArgumentException, all others will only throw IOException
            fs = path.getFileSystem(hadoopConfiguration);

            //resolve path with hdfsPathString first to check whether the path exists => either a real directory or o real file
            //resolvePath() returns fully-qualified variant of the path
            path = fs.resolvePath(path);


            //if recursive approach is requested
            if (recursive) {
                //(heap issues with recursive approach) => using a queue
                Queue<Path> fileQueue = new LinkedList<Path>();

                //add the obtained path to the queue
                fileQueue.add(path);

                //while the fileQueue is not empty
                while (!fileQueue.isEmpty()) {
                    //get the file path from queue
                    Path filePath = fileQueue.remove();

                    //filePath refers to a file
                    if (fs.isFile(filePath)) {
                        filePaths.add(filePath.toString());
                    } else   //else filePath refers to a directory
                    {
                        //list paths in the directory and add to the queue
                        FileStatus[] fileStatuses = fs.listStatus(filePath);
                        for (FileStatus fileStatus : fileStatuses) {
                            fileQueue.add(fileStatus.getPath());
                        } // for
                    } // else

                } // while

            } // if
            else        //non-recursive approach => no heap overhead
            {
                //if the given hdfsPathString is actually referring to a directory
                if (fs.isDirectory(path)) {
                    FileStatus[] fileStatuses = fs.listStatus(path);

                    //loop all file statuses
                    for (FileStatus fileStatus : fileStatuses) {
                        //if the given status is a file, then update the resulting list
                        if (fileStatus.isFile())
                            filePaths.add(fileStatus.getPath().toString());
                    } // for
                } // if
                else        //it is a file then
                {
                    //return the one and only file path to the resulting list
                    filePaths.add(path.toString());
                } // else

            } // else

        } // try
        catch (Exception ex) //will catch all exception including IOException and IllegalArgumentException
        {
            ex.printStackTrace();

            //if some problem occurs return an empty array list
            return new ArrayList<String>();
        } //
        finally {
            //close filesystem; not more operations
            try {
                if (fs != null)
                    fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } // catch

        } // finally


        //return the resulting list; list can be empty if given path is an empty directory without files and sub-directories
        return filePaths;
    } // listFilesFromHDFSPath


    //helper method to list files from the local path in the local file system
    public static List<String> listFilesFromLocalPath(String localPathString, boolean recursive)
    {
        //resulting list of files
        List<String> localFilePaths = new ArrayList<String>();

        //get the Java file instance from local path string
        File localPath = new File(localPathString);


        //this case is possible if the given localPathString does not exit => which means neither file nor a directory
        if (!localPath.exists()) {
            System.err.println("\n" + localPathString + " is neither a file nor a directory; please provide correct local path");

            //return with empty list
            return new ArrayList<String>();
        } // if


        //at this point localPath does exist in the file system => either as a directory or a file


        //if recursive approach is requested
        if (recursive) {
            //recursive approach => using a queue
            Queue<File> fileQueue = new LinkedList<File>();

            //add the file in obtained path to the queue
            fileQueue.add(localPath);

            //while the fileQueue is not empty
            while (!fileQueue.isEmpty()) {
                //get the file from queue
                File file = fileQueue.remove();

                //file instance refers to a file
                if (file.isFile()) {
                    //update the list with file absolute path
                    localFilePaths.add(file.getAbsolutePath());
                } // if
                else   //else file instance refers to a directory
                {
                    //list files in the directory and add to the queue
                    File[] listedFiles = file.listFiles();
                    for (File listedFile : listedFiles) {
                        fileQueue.add(listedFile);
                    } // for
                } // else

            } // while
        } // if
        else        //non-recursive approach
        {
            //if the given localPathString is actually a directory
            if (localPath.isDirectory()) {
                File[] listedFiles = localPath.listFiles();

                //loop all listed files
                for (File listedFile : listedFiles) {
                    //if the given listedFile is actually a file, then update the resulting list
                    if (listedFile.isFile())
                        localFilePaths.add(listedFile.getAbsolutePath());
                } // for
            } // if
            else        //it is a file then
            {
                //return the one and only file absolute path to the resulting list
                localFilePaths.add(localPath.getAbsolutePath());
            } // else
        } // else


        //return the resulting list; list can be empty if given path is an empty directory without files and sub-directories
        return localFilePaths;
    } // listFilesFromLocalPath


    //helper method to get an input stream from file such as HDFS file;
    //if spark is in local mode, then it will get input stream from local file system;
    //otherwise it will get from HDFS;
    private static InputStream inputStreamFromHDFSFilePath(Configuration hadoopConfiguration, String hdfsFilePathString) {
        //initialize input stream with empty string bytes;
        //when converted it will be empty string and in turn empty visits array list
        InputStream inputStream = new ByteArrayInputStream("".getBytes());

        try {
            Path hdfsFilePath = new Path(hdfsFilePathString);    //throws IllegalArgumentException, all others throw IOException
            FileSystem fs = hdfsFilePath.getFileSystem(hadoopConfiguration);
            //resolve hdfsFilePath to check if it is actually exists; if path does not exists IOException will be thrown
            hdfsFilePath = fs.resolvePath(hdfsFilePath);

            //check whether it is a file; if not return bytes from empty string
            if (!fs.isFile(hdfsFilePath))
                return inputStream; // which is defined above as input stream from empty string


            //at this point hdfs file path is actually referring to a file; get the input stream from a file
            inputStream = fs.open(hdfsFilePath);
            //return the resulting input stream
            return inputStream;
        } // try
        catch (Exception ex) {
            ex.printStackTrace();

            //if any problem occurs during input stream generation, return input stream with empty string bytes;
            //which in conversion will be empty string and in turn empty visits array list
            return new ByteArrayInputStream("".getBytes());
        } // catch

    } // inputStreamFromHDFSFilePath


    //overloaded version of the method to get the input stream from the local file path
    public static InputStream inputStreamFromLocalFilePath(String localFilePath)
    {
        //initialize input stream with empty string bytes;
        //when converted it will be empty string and in turn empty visits array list
        InputStream targetStream = new ByteArrayInputStream("".getBytes());

        try {
            //we assume that file exists
            File localFile = new File(localFilePath);

            //if local file does not exist (as a directory or a file); then return input stream with empty string bytes;
            //else if local file does exist, but does not refer to file; then again return input stream with empty string bytes;
            if (!localFile.exists() || (localFile.exists() && !localFile.isFile()))
                return targetStream;


            //at this point localFile exists and it is actually a file
            targetStream = new FileInputStream(localFile);
            //return the resulting input stream
            return targetStream;
        } // try
        catch (Exception ex) {
            ex.printStackTrace();

            //if any problem occurs during input stream generation, return input stream with empty string bytes;
            //which in conversion will be empty string and in turn empty visits array list
            return new ByteArrayInputStream("".getBytes());
        } // catch
    } // inputStreamFromLocalFilePath


    //helper method convert the file's or whatever input stream to string content;
    //some code snippets can cause IOException here, therefore we throw exception;
    //it will be able to get string content both from local file or network file
    private static String stringContentFromInputStream(InputStream inputStream)
    {
        //string builder to contain all lines of the local file or network file
        StringBuilder stringContentBuilder = new StringBuilder("");

        //buffered reader to read input stream
        BufferedReader br = null;

        try {
            //instead of reading whole file input stream at once, we read the file input stream character by character
            //create a buffered reader from input stream via input stream reader
            br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            //variable to hold the int representation of character read
            int charRep = -1;

            //READING LINE BY LINE CAUSES EXCEPTIONS ESPECIALLY FROM INPUT STREAMS; THEREFORE WE READ CHAR BY CHAR
            //read the input stream character by character
            //The character read, as an integer in the range 0 to 65535 (0x00-0xffff), or -1 if the end of the stream has been reached
            while ((charRep = br.read()) != -1) {
                //char 2 bytes;	range => 0 to 65,536 (unsigned)
                stringContentBuilder.append((char) charRep);
            } // while

        } // try
        catch (Exception ex) // will catch all exceptions including "IOException"
        {
            ex.printStackTrace();

            //if any problem occurs during read operation, return an empty string which will yield empty array list of visits at the end
            return "";
        } // catch
        finally {
            //close buffered reader
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } // catch
            } // if
        } // finally


        //return the resulting string from string builder
        return stringContentBuilder.toString();
    } // stringContentFromInputStream


    //this is non-recursive listing of dirs; therefore sub-sub and further deeper level dirs won't be listed
    public static List<String> listDirsFromHDFSNonRecursive(Configuration hadoopConfiguration, String hdfsPath) {
        List<String> dirPaths = new ArrayList<String>();
        FileSystem fs = null;


        try {
            Path path = new Path(hdfsPath);  //throws IllegalArgumentException, all others will only throw IOException
            fs = path.getFileSystem(hadoopConfiguration);

            //if the given hdfsPath is actually directory
            if (fs.isDirectory(path)) {
                dirPaths.add(path.toString());

                FileStatus[] fileStatuses = fs.listStatus(path);

                //loop all file statuses
                for (FileStatus fileStatus : fileStatuses) {
                    //if the given status is a file, then update the resulting list
                    if (fileStatus.isDirectory())
                        dirPaths.add(fileStatus.getPath().toString());
                } // for
            } // if
            //else //it is a file then, do nothing

        } // try
        catch (Exception ex) {
            ex.printStackTrace();

            //return empty list
            return new ArrayList<String>();
        } // catch
        finally {
            try {
                if (fs != null)
                    fs.close();
            } // try
            catch (IOException e) {
                e.printStackTrace();
            } // catch
        } // finally


        return dirPaths;
    } // listDirsFromHDFSNonRecursive


    //helper method to return max measurement date and min measurement date of the dataset files
    public static Tuple2<Date, Date> minMaxMeasurementDates(String localRawFilesDirPathString, boolean recursive, Dataset dataset) {
        List<String> userRawDataFilePaths = Utils.listFilesFromLocalPath(localRawFilesDirPathString, recursive);
        userRawDataFilePaths.removeIf(rawFilePath -> rawFilePath.contains(".xml")); // for CenceMeGPS and Cabspotting

        //min measurement date will be today and max measurement date will unix epoch
        Date minGlobalMeasurementDate = new Date();
        Date maxGlobalMeasurementDate = new Date(0);

        for (String userRawDataFilePath : userRawDataFilePaths) {
            TreeSet<Visit> originalVisits = Preprocessing.sortedOriginalVisitsFromRawFile(userRawDataFilePath, dataset);

            //visits are ordered based on their arrival time
            //so get first and last as min and max measurement date for this user
            Date minUserMeasurementDate = originalVisits.first().getTimeSeriesInstant().getArrivalTime();
            Date maxUserMeasurementDate = originalVisits.last().getTimeSeriesInstant().getArrivalTime();

            //minUserMeasurementDate is before minGlobalMeasurementDate, update global value
            //use the same approach for maxGlobalMeasurementDate
            if (minUserMeasurementDate.before(minGlobalMeasurementDate))
                minGlobalMeasurementDate = minUserMeasurementDate;
            if (maxUserMeasurementDate.after(maxGlobalMeasurementDate))
                maxGlobalMeasurementDate = maxUserMeasurementDate;
        } // for

        return new Tuple2<Date, Date>(minGlobalMeasurementDate, maxGlobalMeasurementDate);
    } // minMaxMeasurementDates


    //helper method to find out trace length in preprocessed user files
    public static Tuple2<Date, Date> minMaxMeasurementDates(String ppUserFilesDirPathString, boolean recursive) {
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(ppUserFilesDirPathString, recursive);

        //min measurement date will be today and max measurement date will unix epoch
        Date minGlobalMeasurementDate = new Date();
        Date maxGlobalMeasurementDate = new Date(0);

        for (String ppUserFilePath : ppUserFilePaths) {
            //merged visits are chronologically ordered, therefore first is smallest and the last the biggest
            InputStream in = Utils.inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileInputStream(in);

            //visits are ordered based on their arrival time
            //so get first and last as min and max measurement date for this user
            Date minUserMeasurementDate = mergedVisits.get(0).getTimeSeriesInstant().getArrivalTime();
            Date maxUserMeasurementDate = mergedVisits
                    .get(mergedVisits.size() - 1).getTimeSeriesInstant().getArrivalTime();

            //minUserMeasurementDate is before minGlobalMeasurementDate, update global value
            //use the same approach for maxGlobalMeasurementDate
            if (minUserMeasurementDate.before(minGlobalMeasurementDate))
                minGlobalMeasurementDate = minUserMeasurementDate;
            if (maxUserMeasurementDate.after(maxGlobalMeasurementDate))
                maxGlobalMeasurementDate = maxUserMeasurementDate;
        } // for

        return new Tuple2<Date, Date>(minGlobalMeasurementDate, maxGlobalMeasurementDate);
    } // minMaxMeasurementDays


    /*
     * Convert a NMEA decimal-decimal degree value into degrees/minutes/seconds
     * First. convert the decimal-decimal value to a decimal:
     * 5144.3855 (ddmm.mmmm) = 51 44.3855 = 51 + 44.3855/60 = 51.7397583 degrees
     *
     * Then convert the decimal to degrees, minutes seconds:
     * 51 degress + .7397583 * 60 = 44.385498 = 44 minutes
     * .385498 = 23.1 seconds
     * Result: 51 44' 23.1"
     *
     * @param NMEADecimalValue NMEA decimal-decimal degree in the format ddmm.mmmm
     * @return String value of the decimal degree, using the proper units
     */
    public static String NMEADecimalToDMS(double NMEADecimalValue) {
        String result = null;
        double degValue = NMEADecimalValue / 100;
        int degrees = (int) degValue;
        double decMinutesSeconds = ((degValue - degrees)) / .60;
        double minuteValue = decMinutesSeconds * 60;
        int minutes = (int) minuteValue;
        double secsValue = (minuteValue - minutes) * 60;
        result = degrees + "\u00B0" + " " + minutes + "' " + String.format("%.1f", secsValue) + "\" ";
        return result;
    } // decimalToDMS

    public static double DMSToDecimal(String DMSValue) {
        return DMSToDecimal(Double.parseDouble(DMSValue));
    } // DMSToDecimal


    //helper method to convert a number in DMS => DegMinSec format (sexagesimal) to decimal degrees
    public static double DMSToDecimal(double DMSValue) {
        //will get its correct value by switch statement
        double dms = 0;

        //change format to (d)dd.mmsssssss
        String dmsString = String.valueOf((int) DMSValue);
        switch (dmsString.length()) {
            case 5:
            case 6:
            case 7: {
                dms = DMSValue / 10000; //e.g. 365212.00000 => 36.5212
                break;
            } // case 5 or 6 or 7
            case 3:
            case 4: {
                dms = DMSValue / 100; //e.g. 3652.12 => 36.5212
                break;
            }
            case 1:
            case 2: {
                dms = DMSValue; //e.g. 36.5212
                break;
            } // case 1 or 2
            default:
                throw new RuntimeException("dmsString.length() is 0 or above 7");
        } // switch


        int deg = 0;
        double frac_part = 0, min_sec = 0, min = 0, sec = 0, min_dd = 0, sec_dd;

        deg = (int) dms;   /* 36 */
        frac_part = dms - deg;   /* 0.5212 */
        min_sec = frac_part * 100;   /* 52.12 */
        min = (int) min_sec;   /* 52 */
        sec = (min_sec - min) * 100;   /* 12 */
        min_dd = min / 60;   /* 0.8667 */
        sec_dd = sec / 3600;   /* 0.0033 */

        return Double.parseDouble(String.format("%.5f%n", deg + min_dd + sec_dd));
    } // DMSToDecimal


    /*
     * Convert a NMEA decimal-decimal degree value into decimal degree
     * Converts the decimal-decimal value to a decimal:
     * 5144.3855 (ddmm.mmmm) = 51 44.3855 = 51 + 44.3855/60 = 51.7397583 degrees
     *
     * @param NMEADecimalValue NMEA decimal-decimal degree in the format ddmm.mmmm
     * @return double decimal degree
     */
    public static double NMEADecimalToDecimal(double NMEADecimalValue) {
        double degValue = NMEADecimalValue / 100;
        int degrees = (int) degValue;
        double decMinutesSeconds = ((degValue - degrees)) / .60;
        return degrees + decMinutesSeconds;
    } // NMEADecimalToDecimal

    //overloaded method with the number of digits to round
    public static double NMEADecimalToDecimal(double NMEADecimalValue, String decimalFormatPattern, RoundingMode roundingMode) {
        double degValue = NMEADecimalValue / 100;
        int degrees = (int) degValue;
        double decMinutesSeconds = ((degValue - degrees)) / .60;

        //format result up to certain number of decimal places
        return format(degrees + decMinutesSeconds, decimalFormatPattern, roundingMode);
    } // NMEADecimalToDecimal


    //helper method to format double number to specific number of decimal places
    public static double format(double number, String decimalFormatPattern, RoundingMode roundingMode) {
        //round the the number up to certain number of decimal places
        DecimalFormat df = new DecimalFormat(decimalFormatPattern); // sample pattern can be "#.#####"
        df.setRoundingMode(roundingMode);
        return Double.parseDouble(df.format(number));
    } // format


//    //helper method to convert list of locations to Weka Instances
//    public static Instances getInstancesFrom(Set<Location> locations)
//    {
//        System.out.println();
//        System.out.println("Transformation: 'Location => DenseInstance' started...");
//
//        //create an empty arraylist first
//        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
//
//        //location has two attributes to be considered latitude and longitude
//        //attribute name will be lat, lng
//        Attribute attributeLat = new Attribute("lat");
//        Attribute attributeLng = new Attribute("lng");
//        attributes.add(attributeLat);
//        attributes.add(attributeLng);
//
//        // Create an empty training set
//        Instances dataset = new Instances("locations", attributes, 10);
//
//        //for each labeledPoint in partition, create an instance
//        //from that labeled point
//        for (Location location : locations)
//        {
//            // Create the instance, number of attributes will be #features
//            DenseInstance instance = new DenseInstance(attributes.size());
//
//            //we only have two attributes, latitude and longitude
//            for (int index = 0; index < attributes.size(); index++)
//            {
//                instance.setValue(0, location.getLatitude());
//                instance.setValue(1, location.getLongitude());
//            } // for
//
//            // add the instance
//            dataset.add(instance);
//        } // for
//
//
//        System.out.println("Transformation: 'Location => DenseInstance' finished...");
//        return dataset;
//    } // getInstancesFrom


    private static Tuple2<Double, Tuple2<Double, Double>> distributedExtractGPSAvgSignLocAvgSignTime(JavaSparkContext sc,
                                                                                                     Dataset dataset,
                                                                                                     List<String> ppUserFilePaths,
                                                                                                     double thresholdT,
                                                                                                     double epsilon,
                                                                                                     int minPts,
                                                                                                     DistanceMeasure dm)
    {
        //count of all significant locations that each user visited
        LongAccumulator countOfAllSignificantLocationsOfUsers = new LongAccumulator();
        sc.sc().register(countOfAllSignificantLocationsOfUsers);

        //total number of visits
        //long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);

        //total residence seconds before pre-processing
        LongAccumulator totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = new LongAccumulator();
        sc.sc().register(totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);

        //double average proportion of time spent by each user in significant places
        DoubleAccumulator sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces = new DoubleAccumulator();
        sc.sc().register(sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces);

        //total number of outlier gps points of users
        LongAccumulator totalOutlierGPSPoints = new LongAccumulator();
        sc.sc().register(totalOutlierGPSPoints);

        //total number of outlier clusters
        //long totalOutlierClusters = 0;

        //broadcast all necessary variables
        Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);
        Broadcast<Double> broadcastEpsilon = sc.broadcast(epsilon);
        Broadcast<Integer> broadcastMinPts = sc.broadcast(minPts);
        Broadcast<DistanceMeasure> broadcastDM = sc.broadcast(dm);
        Broadcast<Double> broadcastThresholdT = sc.broadcast(thresholdT);

        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, ppUserFilePaths.size());
        ppUserFilePathsRDD.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 3466652089392547361L;

            @Override
            public void call(String ppUserFilePath) throws Exception
            {
                //file name is the key for all passed map arguments
                String keyFileName = Utils.fileNameFromPath(ppUserFilePath);

                //obtain merged visits from the map
                List<Visit> mergedVisits = correspondingMergedVisits(broadcastDataset.value(), keyFileName, false);

                //total residence seconds of all visits of this user
                long totalResSecondsOfAllVisitsOfThisUser = 0;

                //total residence seconds of significant visits (visits performed to significant locations) of this user
                long totalResSecondsOfSignificantVisitsOfThisUser = 0;

                //obtain \hat_{F_\mu} of each user
                Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                        = correspondingTuple(broadcastDataset.value(), keyFileName, false);


                //above threshold GPS points
                ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();

                //for each visit's location, if location weighted cumulative Gaussian value is above the threshold,
                //add it to the list
                for (Visit v : mergedVisits)
                {
                    totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                    Location gpsPoint = v.getLocation();
                    double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                    //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                    if (aggregatedWeightedGaussian > broadcastThresholdT.value()
                            || Double.compare(aggregatedWeightedGaussian, broadcastThresholdT.value()) == 0) {
                        aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                    } // if
                } // for
                gpsPointAndWeightedCumulativeGaussianMap.clear(); gpsPointAndWeightedCumulativeGaussianMap = null;


                //update total residence seconds of all visits of all users
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing.add(totalResSecondsOfAllVisitsOfThisUser);


                //CLUSTERING START...
                DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(broadcastEpsilon.value(),
                                                                        broadcastMinPts.value(), broadcastDM.value());

                //cluster aboveThresholdGPSPointsOfThisUser
                List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


                //clear memory
                int aboveThresholdGPSPointsOfThisUserSize = aboveThresholdGPSPointsOfThisUser.size();
                aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;


                //remove the clusters with insufficient number of elements
                clusters.removeIf(locationCluster -> locationCluster.getPoints().size() < broadcastMinPts.value() + 1);

                //update sum of count of all significant locations of each user
                countOfAllSignificantLocationsOfUsers.add(clusters.size());

                //total number of clustered gps points
                int totalGPSPointsInClusters = 0;

                //map to hold gps point and its cluster id
                HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

                //lastClusterID variable for each obtained cluster
                int lastClusterID = 1;

                //obtain iterator for clusters
                Iterator<Cluster<Location>> clusterIterator = clusters.iterator();

                //now for each cluster update list and mp
                while (clusterIterator.hasNext())
                {
                    List<Location> gpsPointsInThisCluster = clusterIterator.next().getPoints();
                    totalGPSPointsInClusters += gpsPointsInThisCluster.size();


                    //now populate location and cluster id map
                    for (Location gpsPoint : gpsPointsInThisCluster)
                    {
                        //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                        gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                    } // for


                    //remove the cluster
                    clusterIterator.remove();

                    //increment clusterID for next cluster
                    lastClusterID++;
                } // for

                //update the total outlier GPS points
                totalOutlierGPSPoints.add(aboveThresholdGPSPointsOfThisUserSize - totalGPSPointsInClusters);
                //CLUSTERING END...

                //get the iterator for merged visits
                Iterator<Visit> mergedVisitsIterator = mergedVisits.iterator();

                //for each visit, if visit is significant, then reroute it its corresponding significant location
                while (mergedVisitsIterator.hasNext())
                {
                    //obtain the visit
                    Visit v = mergedVisitsIterator.next();

                    //obtain the location if this visit
                    Location location = v.getLocation();

                    //check whether this visit's location is within aboveThresholdGPSPoints,
                    //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                    if (gpsPointClusterIDMap.containsKey(location))
                    {
                        long significantVisitResidenceSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                        //update total residence seconds of significant visits
                        totalResSecondsOfSignificantVisitsOfThisUser
                                += significantVisitResidenceSeconds;


                        //update total residence seconds of all visits of all users after preprocessing
                        totalResSecondsOfAllVisitsOfUsersAfterPreprocessing.add(significantVisitResidenceSeconds);
                    } // if


                    //remove the processed visit from the list;
                    //in any case, it should be removed, either its location is contained or not
                    mergedVisitsIterator.remove();

                } // for
                gpsPointClusterIDMap.clear(); gpsPointClusterIDMap = null;


                //update total proportion of time spent by users by a proportion of this user;
                //at the end of for loop we will divide this number by number of users
                double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
                sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces
                        .add(Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser);
            } // call
        });


        System.out.println("(" + thresholdT + "," + totalOutlierGPSPoints.value() + ")"); //+ "," + totalOutlierClusters);


        return new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
                countOfAllSignificantLocationsOfUsers.value() / (double) ppUserFilePaths.size(), 
                //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),

                sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces.value() / ppUserFilePaths.size()
                //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
        ));


    } // distributedExtractGPSAvgSignLocAvgSignTime


    //helper method to extract average significant locations per user and average significant time per user
    //with the given threshold;
    //to speed up processing ppUserFilePaths, merged visits for each file,
    //unique gps points for each file, total residence of all merged visits, pre-computed weighted cumulative Gaussian values
    private static Tuple2<Double, Triple<Double, Double, Double>> extractGPSAvgSignLocAvgSignTime(Dataset dataset,
                                                                                          List<String> ppUserFilePaths,
                                                                                          double thresholdT,
                                                                                          HashMap<String, List<Visit>> userFileNameMergedVisitsMap,
                                                                                          //HashMap<String, HashSet<Location>> userFileNameUniqueGPSPointsMap,
                                                                                          HashMap<String, Long> userFileNameTotalResSecondsOfAllVisitsMap,
                                                                                          //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap,
                                                                                          double epsilon,
                                                                                          int minPts,
                                                                                          DistanceMeasure dm)
    {
        //hash set to hold (global) unique significant locations off all users in the dataset
        //HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        //count of all significant locations that each user visited
        long countOfAllSignificantLocationsOfUsers = 0;


        //total number of visits
        //long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        //long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        //long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces = 0;

        //total number of outlier gps points of users
        long totalOutlierGPSPoints = 0;

        //total number of outlier clusters
        //long totalOutlierClusters = 0;

        //for each user, do gaussian pre-processing
        for (String ppUserFilePath : ppUserFilePaths)
        {
            //String ppUserFilePath = ppUserFilePaths.get(index);
            //file name is the key for all passed map arguments
            String keyFileName = Utils.fileNameFromPath(ppUserFilePath);

            //obtain merged visits from the map
            List<Visit> mergedVisits = correspondingMergedVisits(dataset, keyFileName, false); //userFileNameMergedVisitsMap.get(keyFileName);

            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;

            //obtain pre-computed unique GPS points for this user (which are basically obtained from its merged visits)
            //HashSet<Location> uniqueGPSPoints = userFileNameUniqueGPSPointsMap.get(keyFileName);


            // NEW
            //obtain \hat_{F_\mu} of each user
            Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                    = correspondingTuple(dataset, keyFileName, false); //userFileNameLocationWeightedCumulativeGaussianMap.get(keyFileName);


//            //significant GPS points which pass threshold T
//            HashSet<Location> aboveThresholdGPSPointsOfThisUser = new HashSet<Location>();
//
//            //for each unique location, check whether its \hat_{F_\mu} passes the threshold
//            for(Location uniqueGPSPoint : uniqueGPSPoints)
//            {
//                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(uniqueGPSPoint);
//
//                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
//                if(aggregatedWeightedGaussian > thresholdT
//                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0)
//                {
//                    aboveThresholdGPSPointsOfThisUser.add(uniqueGPSPoint);
//                } // if
//            } // for


            // the same location can be considered more than once in the clustering
            ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();

            //for each visit's location, if location weighted cumulative Gaussian value is above the threshold,
            //add it to the list
            for (Visit v : mergedVisits)
            {
                totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                Location gpsPoint = v.getLocation();
                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                if (aggregatedWeightedGaussian > thresholdT
                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0) {
                    aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                } // if
            } // for
            gpsPointAndWeightedCumulativeGaussianMap.clear();
            gpsPointAndWeightedCumulativeGaussianMap = null;


            //obtained pre-computed total residence of all visits of this user
            //totalResSecondsOfAllVisitsOfThisUser = userFileNameTotalResSecondsOfAllVisitsMap.get(keyFileName);

            //update total residence seconds of all visits of all users
            //totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += totalResSecondsOfAllVisitsOfThisUser;


            //CLUSTERING START...
            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
            //If these areas are located in different parts of the city,
            //the following code will partition the events in different clusters by looking at each location.
            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
            // and we start clustering if there are at least three points close to each other.
            //double epsilon = 100;   // default 100, epsilon in meters for haversine distance
            // = 0.001; // if Kmeans.euclidean, Kmeans.manhattan or Kmeans.cosine distance is used
            //int minPts = 3; // default 3


            DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(epsilon, minPts, dm);

            //cluster aboveThresholdGPSPointsOfThisUser
            List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


            // remove insufficient-length clusters
            clusters.removeIf(locationCluster -> locationCluster.getPoints().size() < minPts + 1);

            countOfAllSignificantLocationsOfUsers += clusters.size();

            int aboveThresholdGPSPointsOfThisUserSize = aboveThresholdGPSPointsOfThisUser.size();
            aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;


            //list of all clustered points
            //List<Location> gpsPointsInClusters = new ArrayList<>();

            int totalGPSPointsInClusters = 0;

            //map to hold gps point and its cluster id
            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

            //lastClusterID variable for each obtained cluster
            int lastClusterID = 1;

            Iterator<Cluster<Location>> clusterIterator = clusters.iterator();

            //now for each cluster update list and mp
            //for (Cluster<Location> cluster : clusters)
            while(clusterIterator.hasNext())
            {
                List<Location> gpsPointsInThisCluster = clusterIterator.next().getPoints();
                //gpsPointsInClusters.addAll(gpsPointsInThisCluster);

                totalGPSPointsInClusters += gpsPointsInThisCluster.size();


                //now populate location and cluster id map
                for (Location gpsPoint : gpsPointsInThisCluster)
                {
                    //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                } // for

                //get rid of this cluster
                clusterIterator.remove();

                //increment clusterID for next cluster
                lastClusterID++;
            } // for
            clusters = null;


            // handle that carefully
            //add outlierGPSPoints to be in their single point clusters
            //aboveThresholdGPSPointsOfThisUser.removeAll(gpsPointsInClusters);

            //HashSet<Location> outlierGPSPoints
            //        = aboveThresholdGPSPointsOfThisUser;

            //List<Location> outlierGPSPoints = aboveThresholdGPSPointsOfThisUser;


            // do not add outlier above threshold gps points as clusters
            //cluster the outliers with minPoints = 0, which will make no outliers,
            //then add resulting cluster to the original clusters
//            DBSCANClusterer<Location> outlierClusterer = new DBSCANClusterer<Location>(epsilon, 0, dm);
//            List<Cluster<Location>> outlierClusters = outlierClusterer.cluster(outlierGPSPoints);
//            for(Cluster<Location> outlierCluster : outlierClusters)
//            {
//                //update the original clusters
//                clusters.add(outlierCluster);
//
//                //now populate location and cluster id map
//                for(Location gpsPoint : outlierCluster.getPoints())
//                {
//                    //if there are the same points, they will be in the same cluster, therefore hash map is not affected
//                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
//                } // for
//
//                //increment clusterID for next outlier cluster
//                lastClusterID ++;
//            } // for
            totalOutlierGPSPoints += aboveThresholdGPSPointsOfThisUserSize - totalGPSPointsInClusters; //outlierGPSPoints.size();
            //totalOutlierClusters += outlierClusters.size();
            //outlierClusters.clear(); outlierClusters = null;


//            for(Location outlierGPSPoint : outlierGPSPoints)
//            {
//                Cluster<Location> thisOutlierGPSPointCluster = new Cluster<>();
//                thisOutlierGPSPointCluster.addPoint(outlierGPSPoint);
//                //System.out.println("lastClusterID = " + lastClusterID);
//
//                //it is possible that no clusters can be generated then, clusterID will be one above
//                //and we will continue from 1
//                clusters.add(thisOutlierGPSPointCluster);
//
//                //also update gps point and cluster id map
//                //gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
//
//                //increment clusterID for next outlier gps point cluster
//                lastClusterID ++;
//            } // for
            //outlierGPSPoints.clear(); outlierGPSPoints = null;
            //aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;
            //gpsPointsInClusters.clear();
            //gpsPointsInClusters = null;


            //index starts from 1
//            int clusterIndex = 1; // is the same as lastClusterID, will the value of lastClusterID at the end of below for loop
//
//            //map to hold cluster id and its average location
//            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();
//
//            //for each cluster, compute its average location and store it in a map
//            for (Cluster<Location> cluster : clusters)
//            {
//                Location averageLocationForThisCluster = Utils.averageLocation(cluster.getPoints());
//                clusterAverageLocationMap.put(clusterIndex, averageLocationForThisCluster);
//
//                //increment cluster index
//                clusterIndex++;
//            } // for


            // add each outlier GPS point to the nearest cluster
            //if there are no significant locations, then do not add
//            if(!clusterAverageLocationMap.isEmpty())
//            {
//                for (Location outlier : outlierGPSPoints)
//                {
//                    double minDist = Double.POSITIVE_INFINITY;
//                    Integer clusterIDWithClosestCentroid = null;
//                    for (Integer clusterID : clusterAverageLocationMap.keySet())
//                    {
//                        Location thisCentroid = clusterAverageLocationMap.get(clusterID);
//                        double dist = outlier.distanceTo(thisCentroid);
//                        if (dist < minDist)
//                        {
//                            minDist = dist;
//                            clusterIDWithClosestCentroid = clusterID;
//                        } // if
//                    } // for
//
//                    //update gpsPointClusterIDMap -> assign
//                    gpsPointClusterIDMap.put(outlier, clusterIDWithClosestCentroid);
//                } // for
//            } // if

            //clusters.clear();
            //clusters = null;
            //CLUSTERING END...


//            //CLUSTERING START...
//            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
//            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
//            //If these areas are located in different parts of the city,
//            //the following code will partition the events in different clusters by looking at each location.
//            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
//            // and we start clustering if there are at least three points close to each other.
//            double epsilon = 100;
//                            // = 0.001; if Kmeans.Euclidean distance is used
//            int minPts = 3; // default 3
//
//            //use tree map to preserve natural ordering of cluster ids
//            TreeMap<Integer, HashSet<Location>> clusterMap
//                    = TestScala.gdbscan(aboveThresholdGPSPointsOfThisUser, ppUserFilePath, epsilon, minPts);
//
//            //get the clustered gps points, clustered gps points will generally be less than aboveThresholdGPSPointsOfThisUser
//            HashSet<Location> gpsPointsInClusters = new HashSet<>();
//            //map to hold gps point and its cluster id
//            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();
//
//            Integer lastClusterID = null;
//            //get the the gps points in all clusters and update gps point and cluster id map
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                gpsPointsInClusters.addAll(cluster);
//
//                //now populate location and cluster id map
//                for(Location gpsPoint : cluster)
//                {
//                    gpsPointClusterIDMap.put(gpsPoint, clusterID);
//                } // for
//
//
//                //update lastClusterID,
//                //at the end of the loop it will get "last cluster ID" of the clusterMap or null if no clusters are found
//                lastClusterID = clusterID;
//            } // for
//
//
//            //add outlierGPSPoints to be in their single point clusters
//            HashSet<Location> outlierGPSPoints = Utils.asymmetricDifference(aboveThresholdGPSPointsOfThisUser, gpsPointsInClusters);
//            for(Location outlierGPSPoint : outlierGPSPoints)
//            {
//                HashSet<Location> thisOutlierGPSPointCluster = new HashSet<>();
//                thisOutlierGPSPointCluster.add(outlierGPSPoint);
//                //System.out.println("lastClusterID = " + lastClusterID);
//                if(lastClusterID == null) lastClusterID = 0;
//                clusterMap.put(++lastClusterID, thisOutlierGPSPointCluster);
//
//                //also update gps point and cluster id map
//                gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
//            } // for
//            aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;
//            gpsPointsInClusters.clear(); gpsPointsInClusters = null;
//            outlierGPSPoints.clear(); outlierGPSPoints = null;
//            //CLUSTERING END...
//
//
//            //for each cluster compute its average location and store it in a map
//            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                Location averageLocationForThisCluster = Utils.averageLocation(cluster);
//                clusterAverageLocationMap.put(clusterID, averageLocationForThisCluster);
//            } // for
//            clusterMap.clear(); clusterMap = null;


            //list to store significant visits
            //ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

            Iterator<Visit> mergedVisitsIterator = mergedVisits.iterator();

            //for each visit, if visit is significant, then reroute it its corresponding significant location
            //for (Visit v : mergedVisits)
            while(mergedVisitsIterator.hasNext())
            {
                Visit v = mergedVisitsIterator.next();

                Location location = v.getLocation();
                //check whether this visit's location is within aboveThresholdGPSPoints,
                //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                if (gpsPointClusterIDMap.containsKey(location))
                {
                    //Integer clusterID = gpsPointClusterIDMap.get(location);
                    //Location significantLocation = clusterAverageLocationMap.get(clusterID);

                    //reroute visit to its significant location
                    //Visit reroutedVisit = new Visit(v.getUserName(), significantLocation, v.getTimeSeriesInstant());
                    //gaussianizedVisits.add(reroutedVisit);


                    //update total residence seconds of significant visits
                    totalResSecondsOfSignificantVisitsOfThisUser
                            += v.getTimeSeriesInstant().getResidenceTimeInSeconds();


                    //update total residence seconds of all visits of all users after preprocessing
                    //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                    //        += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
                } // if


                mergedVisitsIterator.remove();
            } // for
            mergedVisits = null;
            gpsPointClusterIDMap.clear();
            gpsPointClusterIDMap = null;


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;


            //update sum of count of all significant locations of each user
            //countOfAllSignificantLocationsOfUsers += clusterAverageLocationMap.keySet().size();

            //update global unique significant locations hash set
            //globalUniqueSignificantLocations.addAll(clusterAverageLocationMap.values());

            //clear memory
            //clusterAverageLocationMap.clear();
            //clusterAverageLocationMap = null;
            //gaussianizedVisits.clear();
            //gaussianizedVisits = null;
        } // for


        //System.out.println("(" + thresholdT + "," + totalOutlierGPSPoints + ")"); //+ "," + totalOutlierClusters);


        return new Tuple2<Double, Triple<Double, Double, Double>>(thresholdT, Triple.of(
                countOfAllSignificantLocationsOfUsers / (double) ppUserFilePaths.size(), 
                //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),

                sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size(), 
                //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                totalOutlierGPSPoints * 1.0
        ));
    } // extractGPSAvgSignLocAvgSignTime


    //overloaded version which is used to process the data in chunks and then aggregate the results


    //overloaded version of distributedExtractGPSAvgSignLocAvgSignTime() method which computes
    //merged visits, unique gps points and total residence of all visits of each user internally because of a memory constraints
    public static Tuple2<Double, Tuple2<Double, Double>> extractGPSAvgSignLocAvgSignTime(Dataset dataset,
                                                                                         List<String> ppUserFilePaths,
                                                                                         double thresholdT,
                                                                                         HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap,
                                                                                         double epsilon,
                                                                                         int minPts,
                                                                                         DistanceMeasure dm) {
        //hash set to hold (global) unique significant locations off all users in the dataset
        //HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        //count of all significant locations that each user visited
        long countOfAllSignificantLocationsOfUsers = 0;


        //total number of visits
        //long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces = 0;

        //for each user, do gaussian pre-processing
        for (int index = 0; index < ppUserFilePaths.size(); index++) {
            String ppUserFilePath = ppUserFilePaths.get(index);
            //file name is the key for all passed map arguments
            String keyFileName = Utils.fileNameFromPath(ppUserFilePath);


            //obtain visits for this ppUserFilePath
            InputStream in = Utils.inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileInputStream(in);


            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;


            //obtain pre-computed unique GPS points for this user;
            //obtain pre-computed total residence of all visits of this user
            HashSet<Location> uniqueGPSPoints = new HashSet<Location>();
            for (Visit v : mergedVisits) {
                uniqueGPSPoints.add(v.getLocation());
                long thisVisitResSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //update total residence seconds of all visits of this user
                totalResSecondsOfAllVisitsOfThisUser += thisVisitResSeconds;


                //update total residence of all visits of all users before preprocessing
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += thisVisitResSeconds;
            } // for


            // NEW
            //obtain \hat_{F_\mu} of each user
            Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                    = correspondingTuple(dataset, keyFileName, false); //userFileNameLocationWeightedCumulativeGaussianMap.get(keyFileName);


            //significant GPS points which pass threshold T
            HashSet<Location> aboveThresholdGPSPointsOfThisUser = new HashSet<Location>();


            //for each unique location, check whether its \hat_{F_\mu} passes the threshold
            for (Location uniqueGPSPoint : uniqueGPSPoints) {
                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(uniqueGPSPoint);

                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                if (aggregatedWeightedGaussian > thresholdT
                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0) {
                    aboveThresholdGPSPointsOfThisUser.add(uniqueGPSPoint);
                } // if
            } // for


            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
            //If these areas are located in different parts of the city,
            //the following code will partition the events in different clusters by looking at each location.
            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
            // and we start clustering if there are at least three points close to each other.
            //double epsilon = 100;   // default 100, epsilon in meters for haversine distance
            // = 0.001; // if Kmeans.euclidean, Kmeans.manhattan or Kmeans.cosine distance is used
            //int minPts = 3; // default 3


            DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(epsilon, minPts, dm);

            //cluster aboveThresholdGPSPointsOfThisUser
            List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


            //list of all clustered points
            List<Location> gpsPointsInClusters = new ArrayList<>();

            //map to hold gps point and its cluster id
            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

            //lastClusterID variable for each obtained cluster
            int lastClusterID = 1;

            //now for each cluster update list and mp
            for (Cluster<Location> cluster : clusters) {
                List<Location> gpsPointsInThisCluster = cluster.getPoints();
                gpsPointsInClusters.addAll(gpsPointsInThisCluster);


                //now populate location and cluster id map
                for (Location gpsPoint : gpsPointsInThisCluster) {
                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                } // for


                //increment clusterID for next cluster
                lastClusterID++;
            } // for


            // handle that carefully
            //add outlierGPSPoints to be in their single point clusters
            aboveThresholdGPSPointsOfThisUser.removeAll(gpsPointsInClusters);
            HashSet<Location> outlierGPSPoints
                    = aboveThresholdGPSPointsOfThisUser;


            // do not add outlier above threshold gps points as clusters
            //cluster the outliers with minPoints = 0, which will make no outliers,
            //then add resulting cluster to the original clusters
//            DBSCANClusterer<Location> outlierClusterer = new DBSCANClusterer<Location>(epsilon, 0, dm);
//            List<Cluster<Location>> outlierClusters = outlierClusterer.cluster(outlierGPSPoints);
//            for(Cluster<Location> outlierCluster : outlierClusters)
//            {
//                //update the original clusters
//                clusters.add(outlierCluster);
//
//                //now populate location and cluster id map
//                for(Location gpsPoint : outlierCluster.getPoints())
//                {
//                    //if there are the same points, they will be in the same cluster, therefore hash map is not affected
//                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
//                } // for
//
//                //increment clusterID for next outlier cluster
//                lastClusterID ++;
//            } // for
            //totalOutlierGPSPoints += outlierGPSPoints.size();
            //totalOutlierClusters += outlierClusters.size();
            //outlierClusters.clear(); outlierClusters = null;


//            for(Location outlierGPSPoint : outlierGPSPoints)
//            {
//                Cluster<Location> thisOutlierGPSPointCluster = new Cluster<>();
//                thisOutlierGPSPointCluster.addPoint(outlierGPSPoint);
//                //System.out.println("lastClusterID = " + lastClusterID);
//
//                //it is possible that no clusters can be generated then, clusterID will be one above
//                //and we will continue from 1
//                clusters.add(thisOutlierGPSPointCluster);
//
//                //also update gps point and cluster id map
//                gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
//
//
//                //increment clusterID for next outlier gps point cluster
//                lastClusterID ++;
//            } // for
            aboveThresholdGPSPointsOfThisUser.clear();
            aboveThresholdGPSPointsOfThisUser = null;
            gpsPointsInClusters.clear();
            gpsPointsInClusters = null;
            outlierGPSPoints.clear();
            outlierGPSPoints = null;


            //index starts from 1
            int clusterIndex = 1; // is the same as lastClusterID, will the value of lastClusterID at the end of below for loop

            //map to hold cluster id and its average location
            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();

            //for each cluster, compute its average location and store it in a map
            for (Cluster<Location> cluster : clusters) {
                Location averageLocationForThisCluster = Utils.averageLocation(cluster.getPoints());
                clusterAverageLocationMap.put(clusterIndex, averageLocationForThisCluster);

                //increment cluster index
                clusterIndex++;
            } // for


            // add each outlier GPS point to the nearest cluster
            //if there are no significant locations, then do not add
//            if(!clusterAverageLocationMap.isEmpty())
//            {
//                for (Location outlier : outlierGPSPoints)
//                {
//                    double minDist = Double.POSITIVE_INFINITY;
//                    Integer clusterIDWithClosestCentroid = null;
//                    for (Integer clusterID : clusterAverageLocationMap.keySet())
//                    {
//                        Location thisCentroid = clusterAverageLocationMap.get(clusterID);
//                        double dist = outlier.distanceTo(thisCentroid);
//                        if (dist < minDist)
//                        {
//                            minDist = dist;
//                            clusterIDWithClosestCentroid = clusterID;
//                        } // if
//                    } // for
//
//                    //update gpsPointClusterIDMap -> assign
//                    gpsPointClusterIDMap.put(outlier, clusterIDWithClosestCentroid);
//                } // for
//            } // if


            clusters.clear();
            clusters = null;


//            //CLUSTERING START...
//            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
//            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
//            //If these areas are located in different parts of the city,
//            //the following code will partition the events in different clusters by looking at each location.
//            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
//            // and we start clustering if there are at least three points close to each other.
//            double epsilon = 100;
//            // = 0.001; if Kmeans.Euclidean distance is used
//            int minPts = 3; // default 3
//
//            //use tree map to preserve natural ordering of cluster ids
//            TreeMap<Integer, HashSet<Location>> clusterMap
//                    = TestScala.gdbscan(aboveThresholdGPSPointsOfThisUser, ppUserFilePath, epsilon, minPts);
//
//            //get the clustered gps points, clustered gps points will generally be less than aboveThresholdGPSPointsOfThisUser
//            HashSet<Location> gpsPointsInClusters = new HashSet<>();
//            //map to hold gps point and its cluster id
//            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();
//
//            Integer lastClusterID = null;
//            //get the the gps points in all clusters and update gps point and cluster id map
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                gpsPointsInClusters.addAll(cluster);
//
//                //now populate location and cluster id map
//                for(Location gpsPoint : cluster)
//                {
//                    gpsPointClusterIDMap.put(gpsPoint, clusterID);
//                } // for
//
//
//                //update lastClusterID,
//                //at the end of the loop it will get "last cluster ID" of the clusterMap or null if no clusters are found
//                lastClusterID = clusterID;
//            } // for
//
//
//            //add outlierGPSPoints to be in their single point clusters
//            HashSet<Location> outlierGPSPoints = Utils.asymmetricDifference(aboveThresholdGPSPointsOfThisUser, gpsPointsInClusters);
//            for(Location outlierGPSPoint : outlierGPSPoints)
//            {
//                HashSet<Location> thisOutlierGPSPointCluster = new HashSet<>();
//                thisOutlierGPSPointCluster.add(outlierGPSPoint);
//                //System.out.println("lastClusterID = " + lastClusterID);
//                if(lastClusterID == null) lastClusterID = 0;
//                clusterMap.put(++lastClusterID, thisOutlierGPSPointCluster);
//
//                //also update gps point and cluster id map
//                gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
//            } // for
//            aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;
//            gpsPointsInClusters.clear(); gpsPointsInClusters = null;
//            outlierGPSPoints.clear(); outlierGPSPoints = null;
//            //CLUSTERING END...
//
//
//            //for each cluster compute its average location and store it in a map
//            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                Location averageLocationForThisCluster = Utils.averageLocation(cluster);
//                clusterAverageLocationMap.put(clusterID, averageLocationForThisCluster);
//            } // for
//            clusterMap.clear(); clusterMap = null;


            //list to store significant visits
            ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

            //for each visit, if visit is significant, then reroute it its corresponding significant location
            for (Visit v : mergedVisits) {
                Location location = v.getLocation();
                //check whether this visit's location is within aboveThresholdGPSPoints,
                //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                if (gpsPointClusterIDMap.containsKey(location)) {
                    Integer clusterID = gpsPointClusterIDMap.get(location);
                    Location significantLocation = clusterAverageLocationMap.get(clusterID);

                    //reroute visit to its significant location
                    Visit reroutedVisit = new Visit(v.getUserName(), significantLocation, v.getTimeSeriesInstant());
                    gaussianizedVisits.add(reroutedVisit);


                    //update total residence seconds of significant visits
                    totalResSecondsOfSignificantVisitsOfThisUser
                            += reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();


                    //update total residence seconds of all visits of all users after preprocessing
                    totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                            += reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();
                } // if
            } // for
            gpsPointClusterIDMap.clear();
            gpsPointClusterIDMap = null;


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;


            //update sum of count of all significant locations of each user
            countOfAllSignificantLocationsOfUsers += clusterAverageLocationMap.keySet().size();

            //update global unique significant locations hash set
            //globalUniqueSignificantLocations.addAll(clusterAverageLocationMap.values());

            //clear memory
            clusterAverageLocationMap.clear();
            clusterAverageLocationMap = null;
            gaussianizedVisits.clear();
            gaussianizedVisits = null;
        } // for


        return new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
                countOfAllSignificantLocationsOfUsers * 1.0 / (double) ppUserFilePaths.size(), // avg sign loc per user
                //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),

                sumOfProportionsOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size() // avg sign time % per user
                //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
        ));
    } // extractGPSAvgSignLocAvgSignTime


    public static void printGaussianProcessingResults(Dataset dataset,
                                                      List<String> ppUserFilePaths,
                                                      //String localPathString,
                                                      double thresholdT,
                                                      //HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap,
                                                      double epsilon,
                                                      int minPts,
                                                      //double sigma,
                                                      NextPlaceConfiguration npConf,
                                                      double trainSplitFraction, double testSplitFraction
                                                      //boolean listLocalFilesRecursively
    ) {
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };


        //sum of PEs, initially 0
        double sumOfPEsOfUsers = 0;

//        //TESTING GAUSSIANS
//        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
//
//        if(dataset == Dataset.CenceMeGPS)
//        {
//            //sort the file names such that CenceMeLiteLog1 comes before CenceMeLiteLog10
//            ppUserFilePaths.sort(new Comparator<String>() {
//                @Override
//                public int compare(String s1, String s2)
//                {
//                    String[] s1Split = s1.split("Log");
//                    String[] s2Split= s2.split("Log");
//                    return Integer.valueOf(s1Split[1].split("_")[0])
//                            .compareTo(Integer.valueOf(s2Split[1].split("_")[0]));
//                } // compare
//            });
//        } // if


        int fileCount = 0;
        int fileCountWhichHasSmaller1PE = 0;
        int fileCountWhichHasSmallerZeroPointFivePE = 0;
        int fileCountWhichHas1PE = 0;
        int fileCountWhichHasBigger1PE = 0;
        int fileCountWhichHasBiggerEqualTo2PE = 0;
        int fileCountWhichHasSmaller2PE = 0;

        //max and min cumulative gaussian values of the dataset
        double maxCumulativeWeightedGaussianValueOfTheDataset = 0;
        double minCumulativeWeightedGaussianValueOfTheDataset = Double.POSITIVE_INFINITY;


        //hash set to hold (global) unique visited locations of all users in the dataset
        HashSet<Location> globalUniqueLocations = new HashSet<Location>();


        //hash set to hold (global) unique significant locations off all users in the dataset
        HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        long countOfAllSignificantLocationsOfUsers = 0;


        //count of all locations of user; which will be equal to count of all visits of users
        long countOfAllLocationsOfUsers = 0;


        //total number of visits
        long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = 0;

        //total number of outlier GPS points
        long totalOutlierGPSPoints = 0;

        //for each user, do gaussian pre-processing
        for (int index = 0; index < ppUserFilePaths.size(); index++) {
            String ppUserFilePath = ppUserFilePaths.get(index);
            String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
            System.out.println("---------------------- #" + (++fileCount) + ": " + keyFileName
                    + " ---------------------------------");

            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);


            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;


            //update count of all locations of users
            countOfAllLocationsOfUsers += mergedVisits.size();

            HashSet<Location> uniqueGPSPoints = new HashSet<Location>();
            for (Visit v : mergedVisits) {
                //if(!
                uniqueGPSPoints.add(v.getLocation()); //) System.out.println(v.getLocation());

                //update total residence seconds of all visits of this user
                totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();


                //update total residence of all visits of all users before preprocessing
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
            } // for


            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + uniqueGPSPoints.size());
            globalUniqueLocations.addAll(uniqueGPSPoints);
            uniqueGPSPoints.clear();
            uniqueGPSPoints = null;


            // NEW
            //obtain \hat_{F_\mu} of each user
            Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                    = correspondingTuple(dataset, keyFileName, false); //userFileNameLocationWeightedCumulativeGaussianMap.get(keyFileName);


            // convert this to list and cluster visits (their locations actually) instead of unique gps points
            //significant GPS points which pass threshold T
            //HashSet<Location> aboveThresholdGPSPointsOfThisUser = new HashSet<Location>();


            //min and max weighted cumulative gaussian values for this user
            double max = 0;
            double min = Double.POSITIVE_INFINITY;


            //            //significant GPS points which pass threshold T
//            HashSet<Location> aboveThresholdGPSPointsOfThisUser = new HashSet<Location>();
//
//            //for each unique location, check whether its \hat_{F_\mu} passes the threshold
//            for(Location uniqueGPSPoint : uniqueGPSPoints)
//            {
//                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(uniqueGPSPoint);
//
//                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
//                if(aggregatedWeightedGaussian > thresholdT
//                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0)
//                {
//                    aboveThresholdGPSPointsOfThisUser.add(uniqueGPSPoint);
//                } // if
//            } // for


            // the same location can be considered more than once in the clustering
            ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();

            //for each visit's location, if location weighted cumulative Gaussian value is above the threshold,
            //add it to the list
            for (Visit v : mergedVisits) {
                Location gpsPoint = v.getLocation();
                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                if (aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;
                if (aggregatedWeightedGaussian < min) min = aggregatedWeightedGaussian;


                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                if (aggregatedWeightedGaussian > thresholdT
                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0) {
                    aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                } // if
            } // for

            // can have negative effects, only remove location->\hat_F_\mu map of this user
            //gpsPointAndWeightedCumulativeGaussianMap.clear(); gpsPointAndWeightedCumulativeGaussianMap = null;


            System.out.println("\nClustering started...");
            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
            //If these areas are located in different parts of the city,
            //the following code will partition the events in different clusters by looking at each location.
            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
            // and we start clustering if there are at least three points close to each other.
            //double epsilon = 100;   // default 100, epsilon is in meters for haversine distance
            // = 0.001; // if Kmeans.euclidean, Kmeans.manhattan or Kmeans.cosine distance is used
            //int minPts = 19; // default 3

            System.out.println("Total number of above threshold GPS points: " + aboveThresholdGPSPointsOfThisUser.size());

            DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(epsilon, minPts, dm);

            //cluster aboveThresholdGPSPointsOfThisUser
            List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);


            // remove the clusters which contain less than minPts + 1 points
            clusters.removeIf(cluster -> cluster.getPoints().size() < minPts + 1);


            //list of all clustered points
            List<Location> gpsPointsInClusters = new ArrayList<>();

            //map to hold gps point and its cluster id
            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

            //lastClusterID variable for each obtained cluster
            int lastClusterID = 1;

            //now for each cluster update list and mp
            for (Cluster<Location> cluster : clusters) {
                List<Location> gpsPointsInThisCluster = cluster.getPoints();
                gpsPointsInClusters.addAll(gpsPointsInThisCluster);

                //print size of each cluster
                System.out.println("Cluster_" + lastClusterID + " size: " + gpsPointsInThisCluster.size());

                //now populate location and cluster id map
                for (Location gpsPoint : gpsPointsInThisCluster) {
                    //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                } // for


                //increment clusterID for next cluster
                lastClusterID++;
            } // for


            //print number of total locations in all clusters
            System.out.println("Number of locations in all clusters: " + gpsPointsInClusters.size());


            // handle that carefully
            //add outlierGPSPoints to be in their single point clusters
            aboveThresholdGPSPointsOfThisUser.removeAll(gpsPointsInClusters);

            //HashSet<Location> outlierGPSPoints
            //        = aboveThresholdGPSPointsOfThisUser;

            List<Location> outlierGPSPoints = aboveThresholdGPSPointsOfThisUser;

            System.out.println("Number of outlier GPS points: " + outlierGPSPoints.size());

            // do not add outlier above threshold gps points as clusters
            //cluster the outliers with minPoints = 0, which will make no outliers,
            //then add resulting cluster to the original clusters
//            DBSCANClusterer<Location> outlierClusterer = new DBSCANClusterer<Location>(epsilon, 0, dm);
//            List<Cluster<Location>> outlierClusters = outlierClusterer.cluster(outlierGPSPoints);
//            for(Cluster<Location> outlierCluster : outlierClusters)
//            {
//                //update the original clusters
//                clusters.add(outlierCluster);
//
//                //now populate location and cluster id map
//                for(Location gpsPoint : outlierCluster.getPoints())
//                {
//                    //if there are the same points, they will be in the same cluster, therefore hash map is not affected
//                    gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
//                } // for
//
//                //increment clusterID for next outlier cluster
//                lastClusterID ++;
//            } // for
            totalOutlierGPSPoints += outlierGPSPoints.size();
            //totalOutlierClusters += outlierClusters.size();
            //outlierClusters.clear(); outlierClusters = null;


//            for(Location outlierGPSPoint : outlierGPSPoints)
//            {
//                Cluster<Location> thisOutlierGPSPointCluster = new Cluster<>();
//                thisOutlierGPSPointCluster.addPoint(outlierGPSPoint);
//                //System.out.println("lastClusterID = " + lastClusterID);
//
//                //it is possible that no clusters can be generated then, clusterID will be one above
//                //and we will continue from 1
//                clusters.add(thisOutlierGPSPointCluster);
//
//                //also update gps point and cluster id map
//                //gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
//
//                //increment clusterID for next outlier gps point cluster
//                lastClusterID ++;
//            } // for
            outlierGPSPoints.clear();
            outlierGPSPoints = null;
            aboveThresholdGPSPointsOfThisUser.clear();
            aboveThresholdGPSPointsOfThisUser = null;
            gpsPointsInClusters.clear();
            gpsPointsInClusters = null;


            //index starts from 1
            int clusterIndex = 1; // is the same as lastClusterID, will the value of lastClusterID at the end of below for loop

            //map to hold cluster id and its average location
            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();

            //for each cluster, compute its average location and store it in a map
            for (Cluster<Location> cluster : clusters) {
                Location averageLocationForThisCluster = Utils.averageLocation(cluster.getPoints());
                clusterAverageLocationMap.put(clusterIndex, averageLocationForThisCluster);

                //increment cluster index
                clusterIndex++;
            } // for


            // add each outlier GPS point to the nearest cluster
            //if there are no significant locations, then do not add
//            if(!clusterAverageLocationMap.isEmpty())
//            {
//                for (Location outlier : outlierGPSPoints)
//                {
//                    double minDist = Double.POSITIVE_INFINITY;
//                    Integer clusterIDWithClosestCentroid = null;
//                    for (Integer clusterID : clusterAverageLocationMap.keySet())
//                    {
//                        Location thisCentroid = clusterAverageLocationMap.get(clusterID);
//                        double dist = outlier.distanceTo(thisCentroid);
//                        if (dist < minDist)
//                        {
//                            minDist = dist;
//                            clusterIDWithClosestCentroid = clusterID;
//                        } // if
//                    } // for
//
//                    //update gpsPointClusterIDMap -> assign
//                    gpsPointClusterIDMap.put(outlier, clusterIDWithClosestCentroid);
//                } // for
//            } // if


            //CLUSTERING END...
            System.out.println("Clustering ended...\n");


            // TESTING, remove later
            //writeClusters(ppUserFilePath, mergedVisits, clusters, epsilon, minPts);
            // TESTING, remove later


            clusters.clear();
            clusters = null;


            //number of significant locations of this user
            int numSignificantLocationsOfThisUser = clusterAverageLocationMap.keySet().size();


            //list to store significant visits
            ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

            //for each visit, if visit is significant, then reroute it its corresponding significant location
            for (Visit v : mergedVisits) {
                Location location = v.getLocation();
                //check whether this visit's location is within aboveThresholdGPSPoints,
                //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                if (gpsPointClusterIDMap.containsKey(location)) {
                    Integer clusterID = gpsPointClusterIDMap.get(location);
                    Location significantLocation = clusterAverageLocationMap.get(clusterID);

                    //reroute visit to its significant location
                    Visit reroutedVisit = new Visit(v.getUserName(), significantLocation, v.getTimeSeriesInstant());
                    gaussianizedVisits.add(reroutedVisit);


                    //update total residence seconds of significant visits
                    totalResSecondsOfSignificantVisitsOfThisUser
                            += reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();


                    //update total residence seconds of all visits of all users after preprocessing
                    totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                            += reroutedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();
                } // if
            } // for
            //mergedVisits.clear(); mergedVisits = null;
            gpsPointClusterIDMap.clear();
            gpsPointClusterIDMap = null;


            //update global unique significant locations hash set
            globalUniqueSignificantLocations.addAll(clusterAverageLocationMap.values());
            clusterAverageLocationMap.clear();
            clusterAverageLocationMap = null;


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            //if(Double.isNaN(proportionForThisUser)) System.out.println(keyFileName + " is NaN");

            avgProportionOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;


            //update max and min cumulative weighted gaussian values of the dataset
            if (max > maxCumulativeWeightedGaussianValueOfTheDataset)
                maxCumulativeWeightedGaussianValueOfTheDataset = max;
            if (min < minCumulativeWeightedGaussianValueOfTheDataset)
                minCumulativeWeightedGaussianValueOfTheDataset = min;


            //update sum of count of all significant locations of each user
            countOfAllSignificantLocationsOfUsers += numSignificantLocationsOfThisUser;

            //total number of significant visits of this user
            totalNumberOfSignificantVisits += gaussianizedVisits.size();


            System.out.println("Number of significant locations = " + numSignificantLocationsOfThisUser);
            System.out.println("Number of gaussianized visits = " + gaussianizedVisits.size());
            System.out.println("Min cumulative weighted gaussian value = " + min);
            System.out.println("Max cumulative weighted gaussian value = " + max);


            
            //calculate PE of this user
            double peOfUserFile = Utils.peOfPPUserFile(npConf, gaussianizedVisits, trainSplitFraction, testSplitFraction);

            //in the paper, 70% of the dataset have predictability error < 1.0
            //check whether our methods do the same
            if (peOfUserFile < 1.0)
                ++fileCountWhichHasSmaller1PE;
            else if (peOfUserFile > 1.0)
                ++fileCountWhichHasBigger1PE;


            //detect number of files which has PE smaller than 0.5
            if (peOfUserFile < 0.5)
                ++fileCountWhichHasSmallerZeroPointFivePE;


            //check which has bigger 2 or equal to PE and smaller to 2 PE
            if (peOfUserFile < 2.0)
                ++fileCountWhichHasSmaller2PE;
            else if (peOfUserFile > 2.0 || Double.compare(peOfUserFile, 2.0) == 0) {
                ++fileCountWhichHasBiggerEqualTo2PE;
                //filePathsBiggerEqualTo2PE.add(ppUserFilePath);
            }


            //check how many users are not predictable
            if (Double.compare(peOfUserFile, 1.0) == 0)
                ++fileCountWhichHas1PE;


            //update the sum
            sumOfPEsOfUsers += peOfUserFile;
            System.out.println("PE of file with name: " + Utils.fileNameFromPath(ppUserFilePath) + " => " + peOfUserFile);
            System.out.println("Current average PE of all files: " + sumOfPEsOfUsers / fileCount);
            System.out.println(String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 1.0");
            System.out.println(String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 0.5");

            System.out.println("-----------------------------------------------------------------------\n");


            gaussianizedVisits.clear();
            gaussianizedVisits = null;
            //break;
        } // for


        //now calculate average proportion of time spent by each user in significant places
        //by the division of number of users
        avgProportionOfTimeSpentByEachUserInSignificantPlaces = avgProportionOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size();


        //calculate PE_global which is the predictability error of the dataset
        double PE_global = sumOfPEsOfUsers / ppUserFilePaths.size();


        System.out.println("|| totalResSecondsOfAllVisitsOfUsersBeforePreprocessing => " + totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);
        System.out.println("|| totalResSecondsOfAllVisitsOfUsersAfterPreprocessing => " + totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);
        System.out.println("|| Total number of locations (visits) of users before pre-processing => " + countOfAllLocationsOfUsers);
        System.out.println("|| Total number of unique locations before pre-processing => " + globalUniqueLocations.size());


        System.out.println("|| Number of users: " + ppUserFilePaths.size());
        System.out.println("|| Total number of (significant) visits: " + totalNumberOfSignificantVisits);
        System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());
        System.out.println("|| Total number of outlier above threshold GPS points: " + totalOutlierGPSPoints);


        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                //+ (numberOfAllUniqueLocations - allUniqueLocations.size())
                +countOfAllSignificantLocationsOfUsers
                        / (double) ppUserFilePaths.size()
        ));


        System.out.println("|| Average number of (significant) visits per user: "
                + (int) (totalNumberOfSignificantVisits / (double) ppUserFilePaths.size()));
        System.out.println("|| Average residence time in a place D (seconds): "
                + (int) (
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                        //totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                        /// ( 1.0 * globalUniqueSignificantLocations.size() )
                        / (1.0 * countOfAllSignificantLocationsOfUsers)
                /// ( 1.0 * globalUniqueLocations.size())
                /// (1.0 * countOfAllLocationsOfUsers)
        ));
        System.out.println("|| Total trace length in days: " + dataset.traceLengthInDays());


        System.out.println("|| Average proportion of time spent by each user in significant places: "
                //+ String.format("%.2f", totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing)
                + String.format("%.2f", avgProportionOfTimeSpentByEachUserInSignificantPlaces)
                + " %");


        System.out.println("|| " + new Date() + " => PE of " + dataset + " is " + PE_global);
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
                + " % of the dataset has PE < 1.0");
        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
                + " % of the dataset has PE < 0.5");
        System.out.println("|| Number of users which has PE > 1.0: " + fileCountWhichHasBigger1PE);
        System.out.println("|| Number of users which has PE < 2.0: " + fileCountWhichHasSmaller2PE);
        System.out.println("|| Number of users which has PE >= 2.0: " + fileCountWhichHasBiggerEqualTo2PE);

        System.out.println("|| Number of users which are not predictable (has PE == 1.0): " + fileCountWhichHas1PE);


        System.out.println("|| Max cumulative weighted gaussian value of the dataset = " + maxCumulativeWeightedGaussianValueOfTheDataset);
        System.out.println("|| Min cumulative weighted gaussian value of the dataset = " + minCumulativeWeightedGaussianValueOfTheDataset);


        //return new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
        //        countOfAllSignificantLocationsOfUsers / (double) ppUserFilePaths.size(),
        //        //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),
        //
        //        avgProportionOfTimeSpentByEachUserInSignificantPlaces
        //        //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
        //));

    } // printGaussianProcessingResults


    ////helper method to deserialize corresponding user file name and weighted cumulative gaussian map
    //public static Tuple2<String, Map<Location, Double>> correspondingTuple(Dataset dataset, int index)
    //{
    //    if(dataset == Dataset.Cabspotting && (index < 0 || index >= 536))
    //        throw new IllegalArgumentException();
    //    else if(dataset == Dataset.CenceMeGPS && (index < 0 || index >= 19))
    //        throw new IllegalArgumentException();
    //    else if(dataset == Dataset.IleSansFils || dataset == Dataset.DartmouthWiFi)
    //        throw new IllegalArgumentException();
    //
    //
    //    if(dataset == Dataset.Cabspotting)
    //        return (Tuple2<String, Map<Location, Double>>)
    //                Utils.deserialize("raw/Cabspotting/data_all_loc/serialization4/userFileNameLocationWeightedCumulativeGaussianMap"
    //                        + (index + 1) +  ".ser");
    //    else if(dataset == Dataset.CenceMeGPS)
    //        return (Tuple2<String, Map<Location, Double>>)
    //                Utils.deserialize("raw/CenceMeGPS/data_all_loc/serialization2/userFileNameLocationWeightedCumulativeGaussianMap"
    //                        + (index + 1) +  ".ser");
    //    else
    //        throw new IllegalArgumentException();
    //} // correspondingTuple


    public static Map<Location, Double> correspondingTuple(Dataset dataset, String keyFileName, boolean showMessage) {
        if (dataset == Dataset.IleSansFils || dataset == Dataset.DartmouthWiFi)
            throw new IllegalArgumentException();


        if (dataset == Dataset.Cabspotting)
            return (Map<Location, Double>)
                    Utils.deserialize("raw/Cabspotting/data_all_loc/serialization5/userFileNameLocationWeightedCumulativeGaussianMap"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else if (dataset == Dataset.CenceMeGPS)
            return (Map<Location, Double>)
                    Utils.deserialize("raw/CenceMeGPS/data_all_loc/serialization3/userFileNameLocationWeightedCumulativeGaussianMap"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else
            throw new IllegalArgumentException();
    } // correspondingTuple


    public static ArrayList<Visit> correspondingMergedVisits(Dataset dataset, String keyFileName, boolean showMessage) {
        if (dataset == Dataset.IleSansFils || dataset == Dataset.DartmouthWiFi)
            throw new IllegalArgumentException();


        if (dataset == Dataset.Cabspotting)
            return (ArrayList<Visit>)
                    Utils.deserialize("raw/Cabspotting/data_all_loc/serialization7/mergedVisits"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else if (dataset == Dataset.CenceMeGPS)
            return (ArrayList<Visit>)
                    Utils.deserialize("raw/CenceMeGPS/data_all_loc/serialization5/mergedVisits"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else
            throw new IllegalArgumentException();
    } // correspondingTuple


    //helper method to write cluster data to files
    public static void writeClusters(String ppUserFilePath, ArrayList<Visit> mergedVisits,
                                     List<Cluster<Location>> clusters, double epsilon, int minPts) {
        Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits);
        //location and total res seconds map, to map a location and its total residence seconds of visits
        HashMap<Location, Long> locationTotalResSecondsMap = new HashMap<>();

        //for each visit history update locationTotalResSecondsMap
        for (VisitHistory vh : visitHistories) {
            locationTotalResSecondsMap.put(vh.getLocation(), vh.getTotalResidenceSeconds());
        } // for
        visitHistories.clear();
        visitHistories = null;


        //fileID will start from 1 as clusterIndices or IDs
        //number of fileIDs will be the same as as the number of clusterIndices
        int fileID = 1;
        for (Cluster<Location> cluster : clusters) {
            List<Location> locationsInThisCluster = cluster.getPoints();

            StringBuilder sb = new StringBuilder("");
            for (Location loc : locationsInThisCluster) {
                long totalResSeconds = locationTotalResSecondsMap.get(loc);
                sb.append(loc.getLatitude()).append(",").append(loc.getLongitude())
                        .append(",").append(totalResSeconds).append("\n");
            } // for


            Utils.writeToLocalFileSystem(ppUserFilePath,
                    "_cluster_eps=" + epsilon + "_minPts=" + minPts + "_",
                    "" + fileID,
                    ".csv", sb.toString());

            //increment fileID for each cluster
            fileID++;
        } // for
    } // writeClusters


//    //custom distance function for ELKI
//    public class HaversineDistanceFunction extends AbstractNumberVectorDistanceFunction
//    {
//        @Override
//        public double distance(NumberVector point1, NumberVector point2)
//        {
//            double thisLatitude = point1.doubleValue(0); // latitude at dimension index 0
//            double thisLongitude = point1.doubleValue(1); // longitude at dimension index 1
//
//            double otherLatitude = point2.doubleValue(0); // latitude at dimension index 0
//            double otherLongitude = point2.doubleValue(1); // longitude at dimension index 1
//
//            double earthRadius = 3958.7558657441; // earth radius in miles
//            double dLat = Math.toRadians(otherLatitude - thisLatitude);
//            double dLng = Math.toRadians(otherLongitude - thisLongitude);
//            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
//                    + Math.cos(Math.toRadians(thisLatitude))
//                    * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
//                    * Math.sin(dLng / 2);
//            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
//            double dist = earthRadius * c;
//
//            double meterConversion = 1609.344;
//
//            return dist * meterConversion;
//        } // distance
//
//        @Override
//        public boolean isSymmetric() {
//            return true;
//        }
//
//        @Override
//        public boolean isMetric() {
//            return true;
//        }
//
//
//        //Now this domain specific distance function makes only sense for 2-dimensional data.
//        // So we will now specify this, so that ELKI does not try to use it with higher dimensional relations.
//        // For this, we need to override the method getInputTypeRestriction.
//        @Override
//        public SimpleTypeInformation<? super NumberVector> getInputTypeRestriction()
//        {
//            return VectorFieldTypeInformation.typeRequest(NumberVector.class, 2, 2);
//        } // getInputTypeRestriction
//
//
//    } // HaversineDistanceFunction


    //helper method to serialize the object
    public static void serialize(String path, Object object) {
        try {
            FileOutputStream fos =
                    new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(object);
            oos.close();
            fos.close();
            System.out.println("Serialized object data is saved in " + path);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    } // serialize


    //helper method to deserialize object
    public static Object deserialize(String path, boolean showMessage) {
        Object deserializedObject = null;
        try {
            FileInputStream fis = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fis);
            deserializedObject = ois.readObject();
            ois.close();
            fis.close();
            if (showMessage) System.out.println(path + " deserialized");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (ClassNotFoundException c) {
            System.err.println("Class not found");
            c.printStackTrace();
        }

        return deserializedObject;
    } // deserialize


    //method to extract location and weighted cumulative gaussian map
    public static HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap(JavaSparkContext sc,
                                                                                                           Dataset dataset,
                                                                                                           String localPathString,
                                                                                                           double sigma,
                                                                                                           int startIndex,
                                                                                                           int endIndex,
                                                                                                           boolean listLocalFilesRecursively) {
        if (dataset == Dataset.DartmouthWiFi || dataset == Dataset.IleSansFils)
            throw new IllegalArgumentException("this method is only defined for CenceMeGPS and Cabspotting datasets");

        //resulting map
        HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap = new HashMap<>();


        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        //int fileCount = 0;


        //for each user, calculate \hat_F_\mu
        //for(String ppUserFilePath: ppUserFilePaths)
        for (int index = startIndex; index < endIndex /*ppUserFilePaths.size()*/; index++) {
            String ppUserFilePath = ppUserFilePaths.get(index);
            String keyFileName = Utils.fileNameFromPath(ppUserFilePath);

            System.out.println("---------------------- #" + (index + 1) /*(++fileCount)*/
                    + ": " + keyFileName + " ---------------------------------");

            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);


            //hash set of unique locations
            HashSet<Location> uniqueLocations = new HashSet<Location>();

            //list of locations which is the same as the number of visits
            //ArrayList<Location> visitLocations = new ArrayList<>();


            //update unique locations and visit locations
            for (Visit v : mergedVisits) {
                Location loc = v.getLocation();
                uniqueLocations.add(loc);
                //visitLocations.add(loc);
            } // for


            Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits);
            //now find out total residence second of all visits and total residence seconds for each unique location
            long totalResSecondsOfAllVisits_W = 0;

            //map to hold location and its total residence seconds; l -> w_l
            HashMap<Location, Long> locationAndTotalResSeconds_l_w_l_Map = new HashMap<Location, Long>();

            //now for each visit history, update total res seconds of all visits and the map;
            //number of visit histories is the same as number of unique locations
            for (VisitHistory vh : visitHistories) {
                //vh.getTotalResidenceSeconds() is w_l which is weight for the location of a visit history
                totalResSecondsOfAllVisits_W += vh.getTotalResidenceSeconds();

                //now populate the map
                locationAndTotalResSeconds_l_w_l_Map.put(vh.getLocation(), vh.getTotalResidenceSeconds());
            } // for
            visitHistories.clear();
            visitHistories = null;


            int numUniqueLocations = uniqueLocations.size();
            int numPartitions = numUniqueLocations > 100000 ? numUniqueLocations / 100 :
                    (numUniqueLocations > 10000 ? numUniqueLocations / 10 : numUniqueLocations);

            Map<Location, Double> locationAndWeightedCumulativeGaussianMap
                    = Utils.distributedWeightedCumulativeGaussian
                    (sc, numPartitions, uniqueLocations, //visitLocations,
                            locationAndTotalResSeconds_l_w_l_Map, totalResSecondsOfAllVisits_W, sigma);

            //clear memory
            locationAndTotalResSeconds_l_w_l_Map.clear();
            locationAndTotalResSeconds_l_w_l_Map = null;


            userFileNameLocationWeightedCumulativeGaussianMap.put(keyFileName, locationAndWeightedCumulativeGaussianMap);

            System.out.println("-----------------------------------------------------------------------\n");
        } // for

        return userFileNameLocationWeightedCumulativeGaussianMap;
    } // userFileNameLocationWeightedCumulativeGaussianMap


    //helper method to print out hdfs properties
    public static void printHDFSProperties(Configuration hadoopConfiguration) {
        //make the keys ordered in alphabetical order
        TreeMap<String, String> treeMap = new TreeMap<String, String>();

        //iterate and add to the tree map
        for (Map.Entry<String, String> entry : hadoopConfiguration) {
            treeMap.put(entry.getKey(), entry.getValue());
        } // for


        //loop through and print
        for (String paramKey : treeMap.keySet()) {
            System.out.println(paramKey + " = " + treeMap.get(paramKey));
        } // for
    } // printHDFSProperties


    public static void printMeasurementDays(String localRawFilesDirPathString, boolean recursive, Dataset dataset) {
        Tuple2<Date, Date> minMaxMeasurementDates = Utils.minMaxMeasurementDates(localRawFilesDirPathString, recursive, dataset);
        System.out.println("Min measurement date: " + minMaxMeasurementDates._1);
        System.out.println("Max measurement date: " + minMaxMeasurementDates._2);
        System.out.println("Trace length (number of measurement days): "
                + Utils.noOfDaysBetween(minMaxMeasurementDates._1, minMaxMeasurementDates._2));
    } // printMeasurementDays


    //helper method to detect the trace length of preprocessed user files
    public static void printTraceLength(String ppUserFilesDirPathString, boolean recursive) {
        Tuple2<Date, Date> minMaxMeasurementDates = Utils.minMaxMeasurementDates(ppUserFilesDirPathString, recursive);
        System.out.println("Min measurement date: " + minMaxMeasurementDates._1);
        System.out.println("Max measurement date: " + minMaxMeasurementDates._2);
        System.out.println("Trace length (number of measurement days): "
                + Utils.noOfDaysBetween(minMaxMeasurementDates._1, minMaxMeasurementDates._2));
    } // printTraceLength


    //helper method to write data sequentially to HDFS
    private static void writeSequentiallyToHDFS(Configuration hadoopConfiguration, List<Tuple2<String, String>> resultListTuple2,
                                                String nameAppendix) {
        //loop through and write
        for (Tuple2<String, String> resultTuple2 : resultListTuple2) {
            String ppUserFilePath = resultTuple2._1;
            String globalResultString = resultTuple2._2;

            System.out.println("Writing results for " + ppUserFilePath + " .....");

            String fileName = Utils.fileNameFromPath(ppUserFilePath);
            String currentFileExtension = "." + FilenameUtils.getExtension(fileName);

            //now write to hdfs
            Utils.writeToHDFS(hadoopConfiguration, ppUserFilePath, nameAppendix, "", currentFileExtension, globalResultString);
        } // for

    } // writeSequentiallyToHDFS


    //helper method to extract file contents of files from ppUserFilePaths
    //method will return ppUserFilePath and respective file contents pairs
    private static List<Tuple2<String, String>>
    generatePPUserFilePathAndFileContentsPairs(JavaSparkContext sc, List<String> ppUserFilePaths,
                                               int requiredPartitionCount) {
        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, requiredPartitionCount);
        //System.out.println("Pre-processed file paths rDD data count: " + ppUserFilePathsRDD.count());

        //print configured file system
        System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism = " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count = " + requiredPartitionCount);
        System.out.println("Partitions count of the created RDD = " + ppUserFilePathsRDD.partitions().size() + "\n");


        //generate pp user file path and file contents pair rdd
        JavaPairRDD<String, String> ppUserFilePathAndFileContentsPairRDD
                = ppUserFilePathsRDD.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = -4161693627273311013L;

            @Override
            public Tuple2<String, String> call(String ppUserFilePath) throws Exception {
                //get file contents from hdfs
                String fileContents = Utils.
                        fileContentsFromHDFSFilePath(broadcastConf.value().value(), ppUserFilePath,
                                false);

                //return ppUserFilePath and fileContents tuple
                return new Tuple2<String, String>(ppUserFilePath, fileContents);
            } // call
        });

        //now collect pairs, collectAsMap() is also possible
        List<Tuple2<String, String>> list = ppUserFilePathAndFileContentsPairRDD.collect();


        //unpersist rdds and make the ready for GC
        ppUserFilePathsRDD.unpersist();
        ppUserFilePathAndFileContentsPairRDD.unpersist();
        broadcastConf.unpersist();
        ppUserFilePathsRDD = null;
        ppUserFilePathAndFileContentsPairRDD = null;

        /*
        for(Tuple2<String, String> tuple2 : list)
            //println(tuple2._1 + " -> " + tuple2._2);
            println(tuple2);
        */

        //return list tuple2 back
        return list;
    } // generatePPUserFilePathAndFileContentsPairs


    //generates a list tuple2 of all lines of all files from ppUserFilePaths;
    //one line will represent one (merged) visit of  specific user;
    //basically, one merged visit is actually a (u, l, t, d) as presented on the paper
    private static List<String> generateLinesOfAllPPUserFiles(JavaSparkContext sc, List<String> ppUserFilePaths,
                                                              int requiredPartitionCount) {
        //will be used to get hadoop configuration inside transformation functions;
        //since SparkContext is NOT usable inside transformation functions
        final Broadcast<SerializableWritable<Configuration>> broadcastConf
                = sc.broadcast(new SerializableWritable<Configuration>(sc.hadoopConfiguration()));

        //get rdd of preprocessed user file paths
        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, requiredPartitionCount);
        //System.out.println("Pre-processed file paths rDD data count: " + ppUserFilePathsRDD.count());

        //print configured file system
        System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism = " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count = " + requiredPartitionCount);
        System.out.println("Partitions count of the created RDD = " + ppUserFilePathsRDD.partitions().size() + "\n");


        //generate file contents rdd
        // call
        JavaRDD<String> ppUserFileContentsRDD = ppUserFilePathsRDD.map(
                (Function<String, String>) ppUserFilePath
                        -> Utils.fileContentsFromHDFSFilePath(broadcastConf.value().value(), ppUserFilePath,
                        false));


        //one more step to generate rdd of all lines of files
        /*Iterable<String>*/// call
        JavaRDD<String> linesOfAllPPUserFilesRDD = ppUserFileContentsRDD.flatMap(
                (FlatMapFunction<String, String>) fileContents ->
                {
                    //split by "\n"
                    return Arrays.asList(fileContents.split("\n")).iterator();
                });

        //collect all lines
        List<String> linesOfAllPPUserFiles = linesOfAllPPUserFilesRDD.collect();

        //unpersist all rdds and make them ready for GC
        ppUserFilePathsRDD.unpersist();
        ppUserFileContentsRDD.unpersist();
        linesOfAllPPUserFilesRDD.unpersist();
        broadcastConf.unpersist();
        ppUserFilePathsRDD = null;
        ppUserFileContentsRDD = null;
        linesOfAllPPUserFilesRDD = null;


        /*
        for(String line : linesOfAllPPUserFiles)
        {
            println(line);
        } // for
        */


        //return the list
        return linesOfAllPPUserFiles;
    } // generateLinesOfAllPPUserFiles


    //This method returns k'th smallest element in arr[l..r]
    //using randomized QuickSelect algorithm.
    //The worst case time complexity of this solution is still O(n^2) as compared to deterministic QuickSelect.
    //In worst case, the randomized function may always pick a corner element.
    //The expected time complexity of randomized QuickSelect is O(n).
    public static double kthSmallest(Double arr[], int l, int r, int k) {
        // If k is smaller than number of elements in array
        if (k > 0 && k <= r - l + 1) {
            // Partition the array around a random element and
            // get position of pivot element in sorted array
            int pos = randomPartition(arr, l, r);

            // If position is same as k
            if (pos - l == k - 1)
                return arr[pos];

            // If position is more, recur for left sub-array
            if (pos - l > k - 1)
                return kthSmallest(arr, l, pos - 1, k);

            // Else recur for right sub-array
            return kthSmallest(arr, pos + 1, r, k - pos + l - 1);
        } else
            // If k is more than number of elements in array
            throw new RuntimeException("QuickSelect => k is more than number of elements in the array");
    } // kthSmallest


    // Utility method to swap arr[i] and arr[j]
    private static void swap(Double arr[], int i, int j) {
        double temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    // Standard partition process of QuickSort().  It considers
    // the last element as pivot and moves all smaller element
    // to left of it and greater elements to right. This function
    // is used by randomPartition()
    private static int partition(Double arr[], int l, int r) {
        double x = arr[r];
        int i = l;
        for (int j = l; j <= r - 1; j++) {
            if (arr[j] <= x) {
                swap(arr, i, j);
                i++;
            }
        }
        swap(arr, i, r);
        return i;
    }

    // Picks a random pivot element between l and r and
    // partitions arr[l..r] around the randomly picked
    // element using partition()
    private static int randomPartition(Double arr[], int l, int r) {
        int n = r - l + 1;
        int pivot = (int) (Math.random()) * (n - 1);
        swap(arr, l + pivot, r);
        return partition(arr, l, r);
    }


    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    } // countLines


    public static void countLinesOfFilesInADirectory(String dirPath, boolean recursive) {
        List<String> filePaths = Utils.listFilesFromLocalPath(dirPath, recursive);
        int allLinesCount = 0;
        for (String filePath : filePaths) {
            try {
                allLinesCount += Utils.countLines(filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } // for
        System.out.println(allLinesCount);
    } // countLinesOfFilesInADirectory


    //helper method to sort the lines of a file by natural ordering of first property
    public static void generateFileWithSortedLines(String filePath) {
        Scanner scanner = new Scanner(Utils.inputStreamFromLocalFilePath(filePath));
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            lines.add(scanner.nextLine());
        } // while

        System.out.println(lines.size());

        //sort
        lines.sort(new Comparator<String>() {
            @Override
            public int compare(String line1, String line2) {
                try {
                    String[] singleLineComponents1 = line1.split(",");
                    String[] singleLineComponents2 = line2.split(",");

                    Double line1Column1 = Double.parseDouble(singleLineComponents1[0]);
                    Double line2Column1 = Double.parseDouble(singleLineComponents2[0]);

                    Double line1Column2 = Double.parseDouble(singleLineComponents1[1]);
                    Double line2Column2 = Double.parseDouble(singleLineComponents2[1]);

                    //Double line1Column3 = Double.parseDouble(singleLineComponents1[2]);
                    //Double line2Column3 = Double.parseDouble(singleLineComponents2[2]);

                    int result = line1Column1.compareTo(line2Column1);
                    if (result == 0) {
                        result = line1Column2.compareTo(line2Column2);
                        //if(result == 0)
                        //    result = line1Column3.compareTo(line2Column3);
                    } // if
                    return result;
                } catch (Exception ex) {
                    throw new RuntimeException(line1 + "\n" + line2);
                } // catch

            } // compare
        });


        StringBuilder sb1 = new StringBuilder("");
        StringBuilder sb2 = new StringBuilder("");
        lines.forEach(s ->
        {
            String[] stringSplits = s.split(",");

            sb1.append(stringSplits[0]).append(",").append("\n");
            sb2.append(stringSplits[1]).append(",").append("\n");
        });


        Utils.writeToLocalFileSystem(filePath, "_thresholds_", "",
                ".txt", sb1.toString());
        Utils.writeToLocalFileSystem(filePath, "_num_outliers_", "",
                ".txt", sb2.toString());
    } // generateFileWithSortedLines


    public static void generateFileWithSortedLinesAndSumOfProperties(String filePath) {
        Scanner scanner = new Scanner(Utils.inputStreamFromLocalFilePath(filePath));
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            lines.add(scanner.nextLine());
        } // while

        System.out.println(lines.size());

        //sort
        lines.sort(new Comparator<String>() {
            @Override
            public int compare(String line1, String line2) {
                try {
                    String[] singleLineComponents1 = line1.split(",");
                    String[] singleLineComponents2 = line2.split(",");

                    Double line1Column1 = Double.parseDouble(singleLineComponents1[0]);
                    Double line2Column1 = Double.parseDouble(singleLineComponents2[0]);

                    Double line1Column2 = Double.parseDouble(singleLineComponents1[1]);
                    Double line2Column2 = Double.parseDouble(singleLineComponents2[1]);

                    Double line1Column3 = Double.parseDouble(singleLineComponents1[2]);
                    Double line2Column3 = Double.parseDouble(singleLineComponents2[2]);

                    int result = line1Column1.compareTo(line2Column1);
                    if (result == 0) {
                        result = line1Column2.compareTo(line2Column2);
                        if (result == 0)
                            result = line1Column3.compareTo(line2Column3);
                    } // if
                    return result;
                } catch (Exception ex) {
                    throw new RuntimeException(line1 + "\n" + line2);
                } // catch

            } // compare
        });


        ArrayListMultimap<Double, Double> thresholdAndNumOutliersMultiMap = ArrayListMultimap.create();
        ArrayListMultimap<Double, Double> thresholdAndNumOutlierClustersMultiMap = ArrayListMultimap.create();
        for (String line : lines) {
            String[] lineSplits = line.split(",");
            Double threshold = Double.valueOf(lineSplits[0]);
            Double numOutliers = Double.valueOf(lineSplits[1]);
            Double numOutlierClusters = Double.valueOf(lineSplits[2]);

            thresholdAndNumOutliersMultiMap.put(threshold, numOutliers);
            thresholdAndNumOutlierClustersMultiMap.put(threshold, numOutlierClusters);
        } // for


        TreeMap<Double, Double> thresholdAndNumOutliersMap = new TreeMap<>();
        TreeMap<Double, Double> thresholdAndNumOutlierClustersMap = new TreeMap<>();
        //sum up outliers and outlier clusters
        for (Double threshold : thresholdAndNumOutliersMultiMap.keySet()) {
            double sumNumOutliers = 0;
            for (Double numOutliers : thresholdAndNumOutliersMultiMap.get(threshold)) {
                sumNumOutliers += numOutliers;
            } // for

            thresholdAndNumOutliersMap.put(threshold, sumNumOutliers);
        } // for

        //sum up outlier clusters
        for (Double threshold : thresholdAndNumOutlierClustersMultiMap.keySet()) {
            double sumNumOutlierClusters = 0;
            for (Double numOutlierClusters : thresholdAndNumOutlierClustersMultiMap.get(threshold)) {
                sumNumOutlierClusters += numOutlierClusters;
            } // for

            thresholdAndNumOutlierClustersMap.put(threshold, sumNumOutlierClusters);
        } // for


        StringBuilder sb1 = new StringBuilder("");
        StringBuilder sb2 = new StringBuilder("");
        thresholdAndNumOutliersMap.forEach((threshold, numOutliers) ->
        {
            sb1.append(threshold).append(",").append(numOutliers).append("\n");
        });
        thresholdAndNumOutlierClustersMap.forEach((threshold, numOutlierClusters) ->
        {
            sb2.append(threshold).append(",").append(numOutlierClusters).append("\n");
        });


        Utils.writeToLocalFileSystem(filePath, "_thresholds_num_outliers_", "",
                ".txt", sb1.toString());
        Utils.writeToLocalFileSystem(filePath, "_thresholds_num_outlier_clusters_", "",
                ".txt", sb2.toString());
    } // generateFileWithSortedLinesAndSumOfProperties


    //helper method to merge corresponding file lines together
    public static void generateMergedLinesFileFromFiles(String filePath1, String filePath2) {
        Scanner scanner1 = new Scanner(Utils.inputStreamFromLocalFilePath(filePath1));
        List<String> lines1 = new ArrayList<>();
        while (scanner1.hasNextLine()) {
            lines1.add(scanner1.nextLine());
        } // while

        Scanner scanner2 = new Scanner(Utils.inputStreamFromLocalFilePath(filePath2));
        List<String> lines2 = new ArrayList<>();
        while (scanner2.hasNextLine()) {
            lines2.add(scanner2.nextLine());
        } // while

        StringBuilder sb = new StringBuilder("");
        //for each line1 and line2, merge lines
        for (int index = 0; index < lines1.size(); index++) {
            String line1 = lines1.get(index);
            String line2 = lines2.get(index);
            line2 = line2.replace(",", "");
            sb.append("(").append(line1).append(line2).append(")").append("\n");
        } // for

        Utils.writeToLocalFileSystem(filePath1, "_merge_2_", "",
                ".txt", sb.toString());
    } // generateMergedLinesFileFromFiles


    /*
    public static class StringAccumulator extends AccumulatorV2<String, String>
    {
        private StringBuilder sb = new StringBuilder("");

        @Override
        public boolean isZero() {
            return value().isEmpty();
        }

        @Override
        public AccumulatorV2<String, String> copy() {
           StringAccumulator sa = new StringAccumulator();
           sa.add(this.value());
           return sa;
        }

        @Override
        public void reset() {
            sb = new StringBuilder("");
        } // reset

        @Override
        public void add(String v) {
            sb.append(v);
        }

        @Override
        public void merge(AccumulatorV2<String, String> other) {
            sb.append(other.value());
        }

        @Override
        public String value() {
            return sb.toString();
        }

    } // StringAccumulator
    */


    //helper method to draw a k-distance plots of different files
    public static void kDistancePlot(List<String> ppUserFilePaths, int minPts)
    {
        int K = minPts;

        //find the distance to K-th nearest neighbor
        for (int index = 0; index < ppUserFilePaths.size(); index++) {
            String ppUserFilePath = ppUserFilePaths.get(index);
            InputStream in = inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Location> visitLocations = visitLocationsFromPPUserFileInputStream(in);

            ArrayList<Double> kDistances = new ArrayList<>();
            for (int idx = 0; idx < visitLocations.size(); idx++) {
                Double[] distances = new Double[visitLocations.size() - 1]; // no distance between the point and itself
                int i = 0;
                for (int jdx = 0; jdx < visitLocations.size(); jdx++) {
                    if (idx == jdx) continue; // do not find a distance with itself

                    //locationDistanceMultiMap.put(loc1, loc1.distanceTo(loc2));
                    distances[i] = visitLocations.get(idx).distanceTo(visitLocations.get(jdx));
                    i++;
                } // for

                Double kSmallestDistance = kthSmallest(distances, 0, distances.length - 1, K);
                kDistances.add(kSmallestDistance);
            } // for
            visitLocations.clear();
            visitLocations = null;


            //sort in a descending order
            kDistances.sort(new Comparator<Double>() {
                @Override
                public int compare(Double d1, Double d2) {
                    return d2.compareTo(d1);
                }
            });

            //to take first 1000 distances
            //kDistances = Utils.head(kDistances, 1000);

            //GraphPlotter gp = new GraphPlotter(K + "-distance plot");
            //gp.plot(kDistances, 1, K + "-distance plot for " + Utils.fileNameFromPath(ppUserFilePath),
            //        "for point indices ranging between " + 1 + " and " + kDistances.size(), "points", K + "-dist");


            //---- build python file ----
            StringBuilder pfsb = new StringBuilder();
            pfsb.append("import matplotlib.pyplot as plt").append("\n");
            pfsb.append("X = [").append("\n");
            for (int idx = 0; idx < kDistances.size(); idx++) {
                pfsb.append(idx + 1).append(",").append("\n");
            } // for
            pfsb.append("]").append("\n");
            pfsb.append("\n");
            pfsb.append("Y = [").append("\n");
            for (Double dist : kDistances) {
                pfsb.append(dist).append(",").append("\n");
            } // for
            pfsb.append("]").append("\n");
            pfsb.append("\n");
            pfsb.append("plt.plot(X, Y, 'r-o')").append("\n");
            pfsb.append("plt.ylabel('").append(K).append("-dist").append("')").append("\n");
            pfsb.append("plt.xlabel('points')").append("\n");
            pfsb.append("plt.show()").append("\n");

            Utils.writeToLocalFileSystem(ppUserFilePath, "_python_", K + "-dist",
                    ".py", pfsb.toString());
            //---- build python file ----


            //---- build structure for latex plot ----
            StringBuilder latexSb = new StringBuilder("");
            for (int idx = 0; idx < kDistances.size(); idx++) {
                latexSb.append("(").append(idx + 1).append(",").append(kDistances.get(idx)).append(")").append("\n");
            } // for

            Utils.writeToLocalFileSystem(ppUserFilePath, "_latex_", K + "-dist",
                    ".txt", latexSb.toString());
            //---- build structure for latex plot ----

            //only do it for one file
            //break;
        } // for
    } // kDistancePlot


    //cluster radius starting from 10 meters to 250 (2000) meters
    //will plot cluster radius (epsilon) versus average number of clusters (significant locations) per user
    public static void radiusAverageNumberOfClusterPerUserPlot(Dataset dataset, JavaSparkContext sc,
                                                               List<String> ppUserFilePaths, int minPts)
    {
        DistanceMeasure dm = new DistanceMeasure() {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };

        JavaRDD<String> ppUserFilePathsRDD = sc.parallelize(ppUserFilePaths, ppUserFilePaths.size()).cache();
        final Broadcast<Integer> broadcastMinPts = sc.broadcast(minPts);
        final Broadcast<DistanceMeasure> broadcastDM = sc.broadcast(dm);

        LongAccumulator totalNumberOfClusters = new LongAccumulator();
        sc.sc().register(totalNumberOfClusters);


        LinkedHashMap<Double, Double> epsAvgNumberOfClustersPerUserMap = new LinkedHashMap<>();
        for (double chosenEps = 80; chosenEps <= 16000; /*2000;*/ chosenEps += 10) {
            System.out.println("Eps = " + chosenEps + " started ...");

//            int totalNumberOfClusters = 0;
//            //find the distance to K-th nearest neighbor
//            for (int index = 0; index < ppUserFilePaths.size(); index++)
//            {
//                String ppUserFilePath = ppUserFilePaths.get(index);
//                String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
//                ArrayList<Location> visitLocations = correspondingVisitLocations(dataset, keyFileName);
//
//                DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(chosenEps, //broadcastEpsilon.value(),
//                        minPts, dm
//                        //broadcastMinPts.value(),
//                        //broadcastDM.value()
//                );
//
//                //cluster aboveThresholdGPSPointsOfThisUser
//                List<Cluster<Location>> clusters = clusterer.cluster(visitLocations);
//
//                // remove the clusters which contain less than minPts + 1 points
//                clusters.removeIf(cluster -> cluster.getPoints().size() < minPts + 1); //broadcastMinPts.value() + 1);
//
//                totalNumberOfClusters += clusters.size();
//            } // for


            //broadcast variable for chosen epsilon radius
            Broadcast<Double> broadcastChosenEps = sc.broadcast(chosenEps);

            //for each file calculate the clusters and update total number of clusters
            ppUserFilePathsRDD.foreach(new VoidFunction<String>() {
                private static final long serialVersionUID = 9103975529841346516L;

                @Override
                public void call(String ppUserFilePath) throws Exception {
                    String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
                    ArrayList<Location> visitLocations = correspondingVisitLocations(dataset, keyFileName, false);

                    DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(broadcastChosenEps.value(),
                            broadcastMinPts.value(),
                            broadcastDM.value()
                    );

                    //cluster aboveThresholdGPSPointsOfThisUser
                    List<Cluster<Location>> clusters = clusterer.cluster(visitLocations);

                    
                    //remove the clusters which contain less than minPts + 1 points
                    clusters.removeIf(cluster -> cluster.getPoints().size() < minPts + 1); //broadcastMinPts.value() + 1);

                    //update the counter
                    totalNumberOfClusters.add(clusters.size());


                    //clear memory
                    visitLocations.clear();
                    clusters.clear();
                } // call
            });

            double averageNumberOfClustersPerUser = totalNumberOfClusters.value() / (1.0 * ppUserFilePaths.size());
            epsAvgNumberOfClustersPerUserMap.put(chosenEps, averageNumberOfClustersPerUser);

            //reset the counter
            totalNumberOfClusters.reset();

            //unpersist chosenEps broadcast variable
            broadcastChosenEps.unpersist(); //broadcastChosenEps = null;

            System.out.println("Eps = " + chosenEps + " finished ...\n");
        } // for

        ppUserFilePathsRDD.unpersist();
        ppUserFilePathsRDD = null;
        broadcastDM.unpersist();
        broadcastMinPts.unpersist();


        //---- build python file ----
        StringBuilder pfsb = new StringBuilder();
        pfsb.append("import matplotlib.pyplot as plt").append("\n");
        pfsb.append("X = [").append("\n");
        for (double eps : epsAvgNumberOfClustersPerUserMap.keySet()) {
            pfsb.append(eps).append(",").append("\n");
        } // for
        pfsb.append("]").append("\n");
        pfsb.append("\n");
        pfsb.append("Y = [").append("\n");
        for (double eps : epsAvgNumberOfClustersPerUserMap.keySet()) {
            pfsb.append(epsAvgNumberOfClustersPerUserMap.get(eps)).append(",").append("\n");
        } // for
        pfsb.append("]").append("\n");
        pfsb.append("\n");
        pfsb.append("plt.plot(X, Y, 'r-o')").append("\n");
        pfsb.append("plt.ylabel('Average number of clusters per user')").append("\n");
        pfsb.append("plt.xlabel('epsilon radius')").append("\n");
        pfsb.append("plt.show()").append("\n");

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_python_",
                    "_radius_vs_avg_clusters_per_user_",
                    ".py", pfsb.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_python_",
                    "_radius_vs_avg_clusters_per_user_",
                    ".py", pfsb.toString());
        //---- build python file ----

    } // radiusAverageNumberOfClusterPerUserPlot


    public static void radiusAverageNumberOfClusterPerUserPlotDistributed(Dataset dataset, JavaSparkContext sc,
                                                                          List<String> ppUserFilePaths, int minPts)
    {
        DistanceMeasure dm = new DistanceMeasure()
        {
            private static final long serialVersionUID = -7722239876260303382L;

            //Compute the distance between two n-dimensional vectors.
            @Override
            public double compute(double[] point1, double[] point2) {
                double thisLatitude = point1[0];
                double thisLongitude = point1[1];

                double otherLatitude = point2[0];
                double otherLongitude = point2[1];


                double earthRadius = 3958.7558657441; // earth radius in miles
                double dLat = Math.toRadians(otherLatitude - thisLatitude);
                double dLng = Math.toRadians(otherLongitude - thisLongitude);
                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                        + Math.cos(Math.toRadians(thisLatitude))
                        * Math.cos(Math.toRadians(otherLatitude)) * Math.sin(dLng / 2)
                        * Math.sin(dLng / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double dist = earthRadius * c;

                double meterConversion = 1609.344;

                return dist * meterConversion;
            } // compute
        };


        //list to hold the epsilon values
        List<Double> chosenEpsValues = new ArrayList<>();
        for (double chosenEps = 80; chosenEps <= 16000; /*2000;*/ chosenEps += 10) {
            chosenEpsValues.add(chosenEps);
        } // for


        //adjust numSlices accordingly
        int numEpsValues = chosenEpsValues.size();
        int numSlices = numEpsValues > 300000 ? numEpsValues / 1000 : (numEpsValues > 100000 ? numEpsValues / 100 :
                (numEpsValues > 20000 ? numEpsValues / 3 : (numEpsValues > 10000 ? numEpsValues / 2 : numEpsValues)));

        final Broadcast<Integer> broadcastMinPts = sc.broadcast(minPts);
        final Broadcast<DistanceMeasure> broadcastDM = sc.broadcast(dm);
        final Broadcast<List<String>> broadcastPPUserFilePaths = sc.broadcast(ppUserFilePaths);
        final Broadcast<Dataset> broadcastDataset = sc.broadcast(dataset);

        //parallelize list of eps values
        JavaRDD<Double> chosenEpsValuesRDD = sc.parallelize(chosenEpsValues, numSlices);

        //print configured file system
        System.out.println("\nConfigured filesystem = " + Utils.getConfiguredFileSystem(sc.hadoopConfiguration()));
        //prints the number of processors/workers or threads that spark will execute
        System.out.println("Default parallelism: " + sc.defaultParallelism());
        //requiredPartitionCount versus number of partitions of preProcessedFilePathsRDD
        System.out.println("Required partition count: " + numSlices);
        System.out.println("Partitions count of the created RDD: " + chosenEpsValuesRDD.partitions().size() + "\n");


        JavaPairRDD<Double, Double> epsAvgClusterPerUserRDD = chosenEpsValuesRDD.mapToPair(new PairFunction<Double, Double, Double>() {
            private static final long serialVersionUID = -4644738934917893289L;

            @Override
            public Tuple2<Double, Double> call(Double chosenEps) throws Exception {
                int totalNumberOfClusters = 0;
                //for each file find clusters and update the total number of clusters
                for (String ppUserFilePath : broadcastPPUserFilePaths.value()) {
                    String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
                    ArrayList<Location> visitLocations = correspondingVisitLocations(broadcastDataset.value(), keyFileName, false);

                    DBSCANClusterer<Location> clusterer = new DBSCANClusterer<Location>(chosenEps,
                            broadcastMinPts.value(),
                            broadcastDM.value()
                    );

                    //cluster aboveThresholdGPSPointsOfThisUser
                    List<Cluster<Location>> clusters = clusterer.cluster(visitLocations);

                    //remove the clusters which contain less than minPts + 1 points
                    clusters.removeIf(cluster -> cluster.getPoints().size() < minPts + 1); //broadcastMinPts.value() + 1);

                    totalNumberOfClusters += clusters.size();


                    //clear memory
                    visitLocations.clear();
                    clusters.clear();
                } // for

                //obtain average number of clusters per user
                double averageNumberOfClustersPerUser = totalNumberOfClusters / (1.0 * broadcastPPUserFilePaths.value().size());

                //return tuple of eps and avg number of clusters per user
                return new Tuple2<>(chosenEps, averageNumberOfClustersPerUser);
            } // call
        });

        TreeMap<Double, Double> epsAvgNumberOfClustersPerUserMap = new TreeMap<>(epsAvgClusterPerUserRDD.collectAsMap());

        chosenEpsValuesRDD.unpersist();
        chosenEpsValuesRDD = null;
        epsAvgClusterPerUserRDD.unpersist();
        epsAvgClusterPerUserRDD = null;
        broadcastDM.unpersist();
        broadcastMinPts.unpersist();
        broadcastDataset.unpersist();
        broadcastPPUserFilePaths.unpersist();


        //---- build python file ----
        StringBuilder pfsb = new StringBuilder();
        pfsb.append("import matplotlib.pyplot as plt").append("\n");
        pfsb.append("X = [").append("\n");
        for (double eps : epsAvgNumberOfClustersPerUserMap.keySet()) {
            pfsb.append(eps).append(",").append("\n");
        } // for
        pfsb.append("]").append("\n");
        pfsb.append("\n");
        pfsb.append("Y = [").append("\n");
        for (double eps : epsAvgNumberOfClustersPerUserMap.keySet()) {
            pfsb.append(epsAvgNumberOfClustersPerUserMap.get(eps)).append(",").append("\n");
        } // for
        pfsb.append("]").append("\n");
        pfsb.append("\n");
        pfsb.append("plt.plot(X, Y, 'r-o')").append("\n");
        pfsb.append("plt.ylabel('Average number of clusters per user')").append("\n");
        pfsb.append("plt.xlabel('epsilon radius')").append("\n");
        pfsb.append("plt.show()").append("\n");

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_python_",
                    "_radius_vs_avg_clusters_per_user_",
                    ".py", pfsb.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_python_",
                    "_radius_vs_avg_clusters_per_user_",
                    ".py", pfsb.toString());
        //---- build python file ----


    } // radiusAverageNumberOfClusterPerUserPlotDistributed


    public static ArrayList<Location> correspondingVisitLocations(Dataset dataset, String keyFileName, boolean showMessage)
    {
        if (dataset == Dataset.Cabspotting)
            return (ArrayList<Location>)
                    Utils.deserialize("raw/Cabspotting/data_all_loc/serialization6/visitLocations"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else if (dataset == Dataset.CenceMeGPS)
            return (ArrayList<Location>)
                    Utils.deserialize("raw/CenceMeGPS/data_all_loc/serialization4/visitLocations"
                            + "_" + keyFileName + "_" + ".ser", showMessage);
        else
            throw new IllegalArgumentException();
    } // visitLocations


    //helper method to extract number of global unique locations for the files in the dataset
    public static void printGlobalUniqueLocations(Dataset dataset, String dirPathForDatasetFiles, boolean listLocalFilesRecursively)
    {
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(dirPathForDatasetFiles, listLocalFilesRecursively);

        if(dataset == Dataset.CenceMeGPS)
        {
            //sort the file names such that CenceMeLiteLog1 comes before CenceMeLiteLog10
            ppUserFilePaths.sort(new Comparator<String>() {
                @Override
                public int compare(String s1, String s2)
                {
                    String[] s1Split = s1.split("Log");
                    String[] s2Split= s2.split("Log");
                    return Integer.valueOf(s1Split[1].split("_")[0])
                            .compareTo(Integer.valueOf(s2Split[1].split("_")[0]));
                } // compare
            });
        } // if

        //set of global unique locations
        HashSet<Location> globalUniqueLocations = new HashSet<>();

        //for each pp user file path
        for(String ppUserFilePath : ppUserFilePaths)
        {
            //obtain visit locations from serialized data
            ArrayList<Location> visitLocations
                    = Utils.correspondingVisitLocations(dataset, Utils.fileNameFromPath(ppUserFilePath), true);
            globalUniqueLocations.addAll(visitLocations);
        } // for

        System.out.println("Number of unique locations before processing: " + globalUniqueLocations.size());
    } // printGlobalUniqueLocations


    //helper method to sample n elements from the list
    public static <E> List<E> sample(List<E> list, int numberOfElementsToSample)
    {
        //shuffle the list
        Collections.shuffle(list);

        //return top n elements as a sample
        return Utils.head(list, numberOfElementsToSample);

        //it is possible to collect the elements at random indices
    } // sample


    //helper method to abstract the location names in visit histories
    public static void generateVisitsByAbstractingSignificantLocations(Dataset dataset, String datasetDirPath, boolean recursive, boolean isAbsLevel1)
    {
        if(dataset != Dataset.DartmouthWiFi)
            throw new RuntimeException("Only works for DartmouthWiFi dataset");

        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(datasetDirPath, recursive);
        for(String ppUserFilePath : ppUserFilePaths)
        {
            //obtain visits
            InputStream in = inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> significantVisits = visitsFromPPUserFileInputStream(in);

            //string builder to write new visits
            StringBuilder sb = new StringBuilder("");
            String nameAppendix = isAbsLevel1 ? "_abslevel1_" : "_abslevel2_";

            //create visits by abstracting location names
            for(Visit v : significantVisits)
            {
                Location loc = v.getLocation();
                //location names
                //ResBldg56AP13
                //SocBldg4AP4
                //if abstraction level is 1, then split by AP
                //otherwise split by Bldg
                String newLocName;
                if(isAbsLevel1)
                {
                    newLocName = loc.getName().split("AP")[0];
                } // if
                else
                {
                    newLocName = loc.getName().split("Bldg")[0] + "Bldg";
                } // else

                sb.append(v.getUserName()).append(",")
                        .append(newLocName).append(",")
                        .append("-1.0").append(",").append("-1.0") // explicitly make them -1, so only names will be used in comparison
                        .append(",").append(v.getTimeSeriesInstant().getArrivalTime().getTime()).append(",")
                        .append(v.getTimeSeriesInstant().getResidenceTimeInSeconds()).append("\n");
            } // for

            Utils.writeToLocalFileSystem(ppUserFilePath, nameAppendix, "", ".mv", sb.toString());
        } // for

    } // generateVisitsByAbstractingSignificantLocations


    //helper method to pick 4 random locations for each user in Dartmouth WiFi dataset and utilize semantic names
    private void generatedVisitsDaySeconds()
    {
        List<String> paths = Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data", false);
        for(String path : paths)
        {
            InputStream in = Utils.inputStreamFromLocalFilePath(path);
            ArrayList<Visit> visits = Utils.visitsFromPPUserFileInputStream(in);
            String userName = visits.get(0).getUserName() + "_preprocessed";

            Collection<VisitHistory> visitHistories = Utils.createVisitHistories(visits);

            //avg significant locations
            StringBuilder pfsb1 = new StringBuilder();
            pfsb1.append("import matplotlib.pyplot as plt").append("\n");
            pfsb1.append("X = [").append("\n");
            for (VisitHistory vh : visitHistories)
            {
                for(Visit v : vh.getVisits())
                {
                    pfsb1.append(Utils.toDaySeconds(v.getTimeSeriesInstant().getArrivalTime())).append(",").append("\n");
                } // for
            } // for
            pfsb1.append("]").append("\n");
            pfsb1.append("\n");
            pfsb1.append("Y = [").append("\n");
            int count = 1;
            for (VisitHistory vh : visitHistories)
            {
                for(Visit v : vh.getVisits())
                {
                    pfsb1.append(count).append(",").append("\n");
                }

                count ++;
            } // for
            count = 1;
            pfsb1.append("]").append("\n");
            pfsb1.append("\n");
            pfsb1.append("plt.plot(X, Y, 'ro')").append("\n");
            pfsb1.append("plt.ylabel('Visit history')").append("\n");
            pfsb1.append("plt.xlabel('Day seconds')").append("\n");
            pfsb1.append("plt.show()").append("\n");

            //if (dataset == Dataset.DartmouthWiFi)
                Utils.writeToLocalFileSystem("raw/DartmouthWiFi/final_data/plot/_.csv", "_python_",
                        "_visit_history_days_seconds_" + userName,
                        ".py", pfsb1.toString());
            //else if (dataset == Dataset.Cabspotting)
            //    Utils.writeToLocalFileSystem("raw/Cabspotting/final_data/plot/_.csv", "_python_",
            //            "_visit_history_days_seconds_" + userName,
            //            ".py", pfsb1.toString());



            StringBuilder latexSb1 = new StringBuilder("");
            for (VisitHistory vh : visitHistories)
            {
                for(Visit v : vh.getVisits())
                {
                    latexSb1.append("(")
                            .append(Utils.toDaySeconds(v.getTimeSeriesInstant().getArrivalTime()))
                            .append(",").append(count).append(")").append("\n");
                }

                count ++;
            } // for
            count = 1;

            //if (dataset == Dataset.DartmouthWiFi)
                Utils.writeToLocalFileSystem("raw/DartmouthWiFi/final_data/plot/_.csv", "_latex_",
                        "_visit_history_days_seconds_" + userName,
                        ".txt", latexSb1.toString());
            //else if (dataset == Dataset.Cabspotting)
            //    Utils.writeToLocalFileSystem("raw/Cabspotting/final_data/plot/_.csv", "_latex_",
            //            "_visit_history_days_seconds_" + userName,
            //            ".txt", latexSb1.toString());
        } // for
    }


    //helper method to generate refine visits with 4 randomly chosen significant locations
    private void generateVisitsWith4RandomlyChosenLocations()
    {
        List<String> paths4 = new ArrayList<>();
        int significantLocationRestriction = 4;
        List<String> paths = Utils.listFilesFromLocalPath(
                //"raw/Cabspotting/final_data",
                "raw/DartmouthWiFi/final_data",
                false);
        for(String path : paths)
        {
            InputStream in = Utils.inputStreamFromLocalFilePath(path);
            ArrayList<Visit> visits = Utils.visitsFromPPUserFileInputStream(in);
            String userName = visits.get(0).getUserName();

            //HashMultiset<Location> locationHashMultiset = Utils.locationHashMultiset(visits);
            //if(locationHashMultiset.elementSet().size() <= significantLocationRestriction)
            //{
            //    paths4.add(path);
            //} // if

            HashSet<Location> uniqueLocations = new HashSet<>();
            visits.forEach(visit -> uniqueLocations.add(visit.getLocation()));

            HashSet<Location> chosenLocations = new HashSet<>();
            if(uniqueLocations.size() > 4)
            {
                ArrayList<Location> uniqueLocationList = new ArrayList<>(uniqueLocations);

                Random random = new Random();
                while(chosenLocations.size() < 4)
                {
                    int randIndex = random.nextInt(uniqueLocationList.size());
                    chosenLocations.add(uniqueLocationList.get(randIndex));
                } // while
            } // if
            else
                chosenLocations = uniqueLocations;

            TreeSet<Visit> resultVisits = new TreeSet<>();
            for(Visit v : visits)
            {
                if(chosenLocations.contains(v.getLocation()))
                    resultVisits.add(v);
            } // for

            //generate the file contents
            StringBuilder sb = new StringBuilder("");
            for(Visit v : resultVisits)
            {
                sb.append(v.getUserName()).append(",")
                        .append(v.getLocation().getName()).append(",")
                        .append(v.getLocation().getLatitude()).append(",")
                        .append(v.getLocation().getLongitude()).append(",")
                        .append(v.getTimeSeriesInstant().getArrivalTime().getTime()).append(",")
                        .append(v.getTimeSeriesInstant().getResidenceTimeInSeconds()).append("\n");
            } // for

            Utils.writeToLocalFileSystem(
                    //"raw/Cabspotting/final_data_leq4/_.txt",
                    "raw/DartmouthWiFi/final_data_leq4/_.txt",
                    "_",
                    userName + "_preprocessed", ".txt", sb.toString());
        } // for
        //System.out.println("Number of users which has less than 4 significant locations: " + paths4.size());
    }

    //private no-arg constructor to make this class fully static
    private Utils() {
    }

} // class Utils
