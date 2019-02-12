package com.rb.nextplace.distributed;

import com.google.common.collect.*;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * Created by Rufat Babayev on 7/6/2017.
 */
public class Launcher
{
    public static void main(String[] args)
    {
        //you can also use new Date(daySeconds * 100 - TimeZone.getDefault().getOffset(0 * 1000))
        //UNEXPECTED EFFECTS: https://stackoverflow.com/questions/9891881/java-timezone-setdefault-effects
        //this will make sure that all created Dates will be in the Coordinated Universal Time (UTC) TimeZone
        //Baku is UTC + 4, Bonn is UTC + 2
        //timezone issue in predictions, all timestamps on datasets are in Unix epoch which is UTC
        //in cluster mode this can be affected, because nodes may not have default time zone's set
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        //TimeZone.setDefault(TimeZone.getTimeZone("Asia/Baku"));


        //---- NextPlace Algorithm Configuration Default Parameter Values -----
        //embedding parameter m and delay parameter v
        int embeddingParameterM = 3;
        int delayParameterV = 1; //delay parameter v = 1 is used through the experimental evaluations in the paper, m is changed to 1,2,3


        //create NextPlaceConfiguration instance from the above parameters
        //which will be one and only instance throughout the life time of this program
        NextPlaceConfiguration npConf = new NextPlaceConfiguration
                (embeddingParameterM, delayParameterV, DistanceMetric
                        //.ComparativeLoopingDistance,
                        //.ComparativeEuclidean,
                        .Euclidean,
                        NextPlaceConfiguration.NeighborhoodType.EPSILON_NEIGHBORHOOD
                );
        //----------------------------------------------------------------------


        //START OF FULL TEST CONFIGURATION FOR PREDICTABILITY ERROR CALCULATION
        /*

        String master = "spark://str24.kdlan.iais.fraunhofer.de:7077";
        String hadoopHomeDir = "C:\\winutils\\";

        //number of partitions and corresponding number of workers/executor instances/threads
        //#workers refer to #threads in local mode and refers to #executor instances or #processors in cluster mode
        int noOfWorkerThreadsOrNodes = 8; //number of CPUs in my machine
        double trainSplitFraction = 0.9;
        double testSplitFraction = 0.1;


        //boolean values to hold whether to run single machine NextPlace and/or distributed NextPlace
        boolean isSingleMachineMode = false;
        boolean isDistributedMode = false;
        //boolean value to ask to run PE calculation of datasets and PE calculation for each user file
        boolean calculatePEOfADataset = false;
        boolean calculatePEOfEachUserFile = false;


        String hdfsDataDirPathStringCabspotting
                = "hdfs://str24.kdlan.iais.fraunhofer.de/user/mkamp/data/Cabspotting/final_data";
        String hdfsDataDirPathStringCenceMeGPS
                = "hdfs://str24.kdlan.iais.fraunhofer.de/user/mkamp/data/CenceMeGPS/final_data";
        String hdfsDataDirPathStringDartmouthWiFi
                = "hdfs://str24.kdlan.iais.fraunhofer.de/user/mkamp/data/DartmouthWiFi/final_data";
        String hdfsDataDirPathStringIleSansFils
                = "hdfs://str24.kdlan.iais.fraunhofer.de/user/mkamp/data/IleSansFils/final_data";
        String localDataDirPathStringCabspotting
                = "raw/Cabspotting/final_data";
        String localDataDirPathStringCenceMeGPS
                = "raw/CenceMeGPS/final_data";
        String localDataDirPathStringDartmouthWiFi
                = "raw/DartmouthWiFi/final_data";
        String localDataDirPathStringIleSansFils
                = "raw/IleSansFils/final_data";

        boolean isSparkLocalMode = false;
        boolean discardHadoopHomeDirSetup = false;
        Scanner scanner = new Scanner(System.in);


        System.out.println("===== Parameters =====");
        System.out.println("Default value of embedding parameter M: " + embeddingParameterM);
        System.out.print("\nCopy paste above value or type new value of the embedding parameter M (value should be >= 1): ");
        embeddingParameterM = scanner.nextInt();


        System.out.println("\nDefault value of delay parameter V (which is also default value in the paper): " + delayParameterV);
        System.out.print("\nCopy paste above value or type new value of the delay parameter V (value should be >= 1): ");
        delayParameterV = scanner.nextInt();




        System.out.print("\nChoose run mode [type 1 for single-machine mode, type 2 for distributed mode or type 3 for both modes]: ");
        String npRunModeChoice = scanner.next();

        if(npRunModeChoice.equalsIgnoreCase("1"))
            isSingleMachineMode = true;
        else if(npRunModeChoice.equalsIgnoreCase("2"))
            isDistributedMode = true;
        else if(npRunModeChoice.equalsIgnoreCase("3"))
        {
            isSingleMachineMode = true;
            isDistributedMode = true;
        } // else if
        else
        {
            System.out.println("Enter 0 or 1 or 2 or 3 !!!");
            System.exit(0);
        } // else


        if(isDistributedMode)
        {
            System.out.print("\nDo you want to run Apache Spark in a local mode [ yes no ]: ");
            String sparkModeChoice = scanner.next();

            if (sparkModeChoice.equalsIgnoreCase("no"))
            {
                isSparkLocalMode = false;
            } // if
            else if (sparkModeChoice.equalsIgnoreCase("yes"))
            {
                isSparkLocalMode = true;

                System.out.println("\nDefault local data dir for " + Dataset.Cabspotting + " => " + localDataDirPathStringCabspotting);
                System.out.print("\nCopy paste above dir or type path for new local data dir of " + Dataset.Cabspotting +  ": ");
                localDataDirPathStringCabspotting = scanner.next();

                System.out.println("\nDefault local data dir for " + Dataset.CenceMeGPS + " => " + localDataDirPathStringCenceMeGPS);
                System.out.print("\nCopy paste above dir or type path for new local data dir of " + Dataset.CenceMeGPS +  ": ");
                localDataDirPathStringCenceMeGPS = scanner.next();

                System.out.println("\nDefault local data dir for " + Dataset.DartmouthWiFi + " => " + localDataDirPathStringDartmouthWiFi);
                System.out.print("\nCopy paste above dir or type path for new local data dir of " + Dataset.DartmouthWiFi +  ": ");
                localDataDirPathStringDartmouthWiFi = scanner.next();

                System.out.println("\nDefault local data dir for " + Dataset.IleSansFils + " => " + localDataDirPathStringIleSansFils);
                System.out.print("\nCopy paste above dir or type path for new local data dir of " + Dataset.IleSansFils +  ": ");
                localDataDirPathStringIleSansFils = scanner.next();

                //there is no master in local mode
                master = "none";

                //adjust number of nodes/threads
                System.out.println("\nDefault number of worker threads/nodes is): " + noOfWorkerThreadsOrNodes);
                System.out.print("\nCopy paste above number or type new number of worker threads/nodes: ");
                noOfWorkerThreadsOrNodes = scanner.nextInt();

                System.out.println("\nDefault train split fraction is " + trainSplitFraction);
                System.out.print("\nCopy paste above number or type new train split fraction: ");
                trainSplitFraction = scanner.nextDouble();

                System.out.println("\nDefault test split fraction is " + testSplitFraction);
                System.out.print("\nCopy paste above number or type new test split fraction: ");
                testSplitFraction = scanner.nextDouble();


            //adjust output dir
            //System.out
            //        .println("\noutput .txt files will be created by driver node "
            //                + "using java FileWriter.");
            //System.out.println("\nDefault output dir: " + OUTPUT_DIR);
            //System.out
            //        .print("\nCopy paste above dir or type path for new output dir [do not forget / at the end]: ");
            //OUTPUT_DIR = scanner.next();


                System.out
                        .println("\nThere is a line in a code >> System.setProperty(\"hadoop.home.dir\", \"C:\\winutils\\\"); <<");
                System.out
                        .print("Do you want to discard that line [ yes no ]: ");
                String choice = scanner.next();

                if (choice.equalsIgnoreCase("no")) {
                    System.out.println("\nDefault HADOOP_HOME_DIR: "
                            + hadoopHomeDir);
                    System.out
                            .print("\nCopy paste above dir or type new hadoop home dir path: ");
                    hadoopHomeDir = scanner.next();
                    discardHadoopHomeDirSetup = false;
                } // if
                else if (choice.equalsIgnoreCase("yes")) {
                    discardHadoopHomeDirSetup = true;
                } // else if
            } // else if
            else
            {
                System.out.println("Enter yes or no !!!");
                System.exit(0);
            } // else


            //if not a local mode, then...
            if (!isSparkLocalMode)
            {
                System.out.println("Make sure all files are ready in HDFS .....");

                // Scanner for input prompt
                try
                {
                    System.out.println("\nSample hdfs data dir style for " + Dataset.Cabspotting + " => " + hdfsDataDirPathStringCabspotting);
                    System.out.print("\nType path for real hdfs data dir of " + Dataset.Cabspotting + ": ");
                    hdfsDataDirPathStringCabspotting = scanner.next();

                    System.out.println("\nSample hdfs data dir style for " + Dataset.CenceMeGPS + " => " + hdfsDataDirPathStringCenceMeGPS);
                    System.out.print("\nType path for real hdfs data dir of " + Dataset.CenceMeGPS + ": ");
                    hdfsDataDirPathStringCenceMeGPS = scanner.next();


                    System.out.println("\nSample hdfs data dir style for " + Dataset.DartmouthWiFi + " => " + hdfsDataDirPathStringDartmouthWiFi);
                    System.out.print("\nType path for real hdfs data dir of " + Dataset.DartmouthWiFi + ": ");
                    hdfsDataDirPathStringDartmouthWiFi = scanner.next();

                    System.out.println("\nSample hdfs data dir style for " + Dataset.IleSansFils + " => " + hdfsDataDirPathStringIleSansFils);
                    System.out.print("\nType path for real hdfs data dir of " + Dataset.IleSansFils + ": ");
                    hdfsDataDirPathStringIleSansFils = scanner.next();

                    //System.out
                    //        .println("\noutput .txt files will be created by driver node "
                    //            + "using java FileWriter.");
                    //System.out.println("\nDefault output dir: " + OUTPUT_DIR);
                    //System.out
                    //        .print("\nCopy paste above dir or type path for new output dir [do not forget / at the end]: ");
                    //OUTPUT_DIR = scanner.next();



                    //adjust the master url
                    System.out.println("\nSample MASTER node url:port combination => " + master);
                    System.out.print("\nType real url:port combination of your MASTER node: ");
                    master = scanner.next();

                    //adjust number of nodes/threads
                    System.out.println("\nDefault number of worker threads/nodes is): " + noOfWorkerThreadsOrNodes);
                    System.out.print("\nCopy paste above number or type new number of worker threads/nodes: ");
                    noOfWorkerThreadsOrNodes = scanner.nextInt();


                    System.out.println("\nDefault train split fraction is " + trainSplitFraction);
                    System.out.print("\nCopy paste above number or type new train split fraction: ");
                    trainSplitFraction = scanner.nextDouble();

                    System.out.println("\nDefault test split fraction is " + testSplitFraction);
                    System.out.print("\nCopy paste above number or type new test split fraction: ");
                    testSplitFraction = scanner.nextDouble();


                    System.out
                            .println("\nThere is a line in a code >> System.setProperty(\"hadoop.home.dir\", \"C:\\winutils\\\"); <<");
                    System.out
                            .print("Do you want to discard that line {if you are not in a local mode, then it will probably be yes} [ yes no ]: ");
                    String choice = scanner.next();

                    if (choice.equalsIgnoreCase("no")) {
                        System.out.println("\nDefault HADOOP_HOME_DIR: "
                                + hadoopHomeDir);
                        System.out
                                .print("\nCopy paste above dir or type new hadoop home dir path: ");
                        hadoopHomeDir = scanner.next();
                        discardHadoopHomeDirSetup = false;
                    } // if
                    else if (choice.equalsIgnoreCase("yes")) {
                        discardHadoopHomeDirSetup = true;
                    } // else if
                    else {
                        System.out.println("Enter yes or no !!!");
                        System.exit(0);
                    } // else

                } // try
                catch (Exception ex) {
                    ex.printStackTrace(System.err);
                } // catch

            } // if not a local mode

        } // if nextPlace algorithm is requested in distributed mode

        //close scanner
        scanner.close();

        //System.out.println("EXPLICIT SYSTEM EXIT");
        //System.exit(0);


        //----------------------------------------------------------------------------------------------------

        //if single machine mode is enabled
        if(isSingleMachineMode)
        {
                //calculation of PE
                testSingleMachinePECalculation(npConf, trainSplitFraction, testSplitFraction,
                        localDataDirPathStringCenceMeGPS, localDataDirPathStringCabspotting,
                        localDataDirPathStringDartmouthWiFi, localDataDirPathStringIleSansFils);
        } // if


        //if distributed is requested
        if(isDistributedMode)
        {
            //number of partitions and corresponding number of workers/executor instances/threads
            //int requiredPartitionCount = 8; // => number of processors in my machine
            //int noOfWorkers = requiredPartitionCount;

            //when hadoopHomeDir is not defined, causes IOException
            JavaSparkContext sc = initializeSpark(//"none"*
                                                master, isSparkLocalMode,   //true,
                                                discardHadoopHomeDirSetup,      //false,
                                                hadoopHomeDir,                  //"C:\\winutils\\",
                                                noOfWorkerThreadsOrNodes);

            //printHDFSProperties(sc.hadoopConfiguration());

            //you can disable the _SUCCESS file by setting the following on the hadoop configuration of the Spark context.
            //sc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
            //Note, that you may also want to disable generation of the metadata files with:
            //sc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false");


                //check distributed PE calculation
                if(isSparkLocalMode) // local mode will use local data dirs
                    testDistributedPECalculation(sc, npConf, trainSplitFraction, testSplitFraction, localDataDirPathStringCenceMeGPS, localDataDirPathStringCabspotting,
                            localDataDirPathStringDartmouthWiFi, localDataDirPathStringIleSansFils);
                else    // it is a cluster mode
                testDistributedPECalculation(sc, npConf, trainSplitFraction, testSplitFraction, hdfsDataDirPathStringCenceMeGPS,
                        hdfsDataDirPathStringCabspotting, hdfsDataDirPathStringDartmouthWiFi, hdfsDataDirPathStringIleSansFils);


            //It automatically closes sparkContext, no need for below calls
            //stop and close sparkContext
            //sc.stop();
            //sc.close();
            //sc = null;

        } // if
        //------------------------------------------------------------------------------------------

        System.exit(0);
        */ //END OF FULL TEST CONFIGURATION FOR PREDICTABILITY ERROR CALCULATION






        //-----  LOCAL TEST MODE -----


        //DATA PROCESSING

        //sort the xml file and get sorted xml file path
        //one xml node looks like the following:
        //<cab id="enyenewl" updates="2414"/>
        //we will sort elements with <cab/> by "id" in alphabetical order


        /*
        //generate sorted xml file path of the given dataset
        String sortedXMLFilePath = Preprocessing.sortXmlFileInPath("raw/Cabspotting/_cabs.xml", Dataset.Cabspotting);

        //GPS sampling interval for Cabspotting dataset is 60 seconds
        //therefore visits within the interval 60 seconds will be merged to one visit
        int deltaThresholdInSeconds = 60; //60 is appropriate for Cabspotting dataset
        double significantLocationThreshold = 4.5E-8;
        double eps = 200; // 200 meters radius (DBSCAN)
        int mPts = 18;  // min points (DBSCAN)

        //the list of pre-processed user file paths
        ArrayList<String> ppUserFilePaths = Preprocessing.generatePPUserFilePaths(sc, sortedXMLFilePath, Dataset.Cabspotting,
                                                    deltaThresholdInSeconds, significantLocationThreshold, 10, eps, mPts);
        System.exit(0);
        */


        //------------------------------------------------------------------------------------------

        /*
        //sorted xml file path of CenceMeGPS dataset
        String sortedXMLFilePath = "raw/CenceMeGPS/_cenceme.xml";
        int deltaThresholdInSeconds = 180; //180 is appropriate for CenceMeGPS dataset as stated in the paper
        double significantLocationThreshold = 2.6E-6;
        double eps = 100; // 100 meters radius (DBSCAN)
        int mPts = 18;  // min points (DBSCAN)

        //get the pre-processed file paths
        //where the method will pre-process the given dataset internally
        ArrayList<String> ppUserFilePaths = Preprocessing.generatePPUserFilePaths(sc, sortedXMLFilePath, Dataset.CenceMeGPS,
                                    deltaThresholdInSeconds, significantLocationThreshold, 10, eps, mPts);
        //System.exit(0);
        */


        //------------------------------------------------------------------------------------------


        /*
        //directory path for raw data files
        String rawFilesDirPath = "raw/DartmouthWiFi2";
        int deltaThresholdInSeconds = 300; //300 is appropriate for DartmouthWiFi dataset as stated in the paper
        double significantLocationThreshold = 20;

        //get the pre-processed file paths
        //where the method will pre-process the given dataset internally
        ArrayList<String> preprocessedUserFilePaths = Preprocessing.generatePPUserFilePaths(sc, rawFilesDirPath, Dataset.DartmouthWiFi,
                                deltaThresholdInSeconds, significantLocationThreshold, 10, false,
                                null, null);
        //prePreProcessingForDartmouthWiFi();
        System.exit(0);
        */

        //------------------------------------------------------------------------------------------

        /*
        //directory path for raw data files
        String rawFilesDirPath = "raw/IleSansFils/unprocessed";
        int deltaThresholdInSeconds = 300; //300 is appropriate for IleSansFils dataset as stated in the paper
        double significantLocationThreshold = 20;

        //get the pre-processed file paths
        //where the method will pre-process the given dataset internally
        ArrayList<String> ppUserFilePaths = Preprocessing.generatePPUserFilePaths(sc, rawFilesDirPath, Dataset.IleSansFils,
                                deltaThresholdInSeconds, significantLocationThreshold, 10, false);
        System.exit(0);
        */

        //------------------------------------------------------------------------------------------
        //DATA PROCESSING



        //for finding application run time
        long startTime = System.nanoTime();

        //number of workers/executor instances/threads;
        //number of partitions will be different for each dataset
        int noOfWorkers = 8; // => number of CPUs in my machine

        //when hadoopHomeDir is not defined, causes IOException
        JavaSparkContext sc = initializeSpark("none",
                true,
                false,
                "C:\\winutils\\",
                noOfWorkers);




        //A SIMPLE WAY OBTAINING LOCATION PREDICTIONS RESULTS AT THE GIVEN TIME T AND DELTA T
        /*
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(
                //"raw/IleSansFils/final_data",
                "raw/DartmouthWiFi/final_data",
                //"raw/Cabspotting/final_data",
                //"raw/CenceMe GPS/final_data",
                false);
        for(String ppUserFilePath : ppUserFilePaths)
        {
            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> significantVisits = Utils.visitsFromPPUserFileContents(fileContents);

            StringBuilder sb = new StringBuilder("");
            String finalResult =
                    Utils.resultsFromNextPlaceRun(sb, Utils.fileNameFromPath(ppUserFilePath),
                            significantVisits, npConf, false, 0.99999999, 0.11111111,
                            6400, 3600, 900);

            //System.out.println(finalResult);

            Utils.writeToLocalFileSystem(ppUserFilePath, "_NextPlace_Results_",
                    "", ".csv", finalResult);
            //break;
        } // for
        */




        // ----- CALCULATION OF PREDICTABILITY ERROR IN A LOCAL MODE -----

        //List<String> paths = Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data", false);
        //paths.addAll(Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data/discarded", false));
        Utils.peOfADatasetDistributed(Dataset.DartmouthWiFi, sc,
                "raw/DartmouthWiFi/final_data",
                //paths,
                false,
                npConf,
                //0.99999999,
                //0.5,
                //0.7,
                0.9,
                //0.00000001
                //0.5
                //0.3
                0.1
                );


        //List<String> paths = Utils.listFilesFromLocalPath("raw/IleSansFils/final_data", false);
        //paths.addAll(Utils.listFilesFromLocalPath("raw/IleSansFils/final_data/discarded", false));
        Utils.peOfADatasetDistributed(Dataset.IleSansFils, sc,
                "raw/IleSansFils/final_data",
                //paths,
                false,
                npConf,
                //0.99999999,
                //0.5,
                //0.7,
                0.9,
                //0.00000001
                //0.5
                //0.3
                0.1
                );


        //System.out.println("Embedding parameter m = " + embeddingParameterM);
        //List<String> paths = Utils.listFilesFromLocalPath("raw/CenceMeGPS/final_data", false);
        //paths.addAll(Utils.listFilesFromLocalPath("raw/CenceMeGPS/final_data/discarded", false));
        Utils.peOfADatasetDistributed(Dataset.CenceMeGPS, sc,
                "raw/CenceMeGPS/final_data",
                false,
                //paths,
                npConf,
                //0.99999999,
                //0.5,
                //0.7,
                0.9,
                //0.00000001
                //0.5
                //0.3
                0.1
                );


        Utils.peOfADatasetDistributed(Dataset.Cabspotting, sc,
                "raw/Cabspotting/final_data",
                false,
                npConf,
                //0.99999999,
                //0.5,
                //0.7,
                0.9,
                //0.00000001
                //0.5
                //0.3
                0.1
        );
        System.exit(0);
        // ----- CALCULATION OF PREDICTABILITY ERROR IN A LOCAL MODE -----




        // --- CALCULATION OF PREDICTION PRECISION IN A LOCAL MODE -----

        //20 random seeds for 20 runs
        long[] seeds = new long[]{8L };
        //, 97L, 129L, 5632L, 34234L, 496217L, 2068041L, 38714554L, 9355793L, 819108L, 74598L, 6653L,
        //505L, 78L, 3L, 24L, 305L, 7787L, 12633L, 217812L};

        //list preprocessed user file paths from local dir path
        //Dataset dts
                //= Dataset.Cabspotting;
                //= Dataset.CenceMeGPS;
                //= Dataset.DartmouthWiFi;
                //= Dataset.IleSansFils;
        //List<String> ppUserFilePaths
                //= Utils.listFilesFromLocalPath("raw/Cabspotting/final_data", false);
                //= Utils.listFilesFromLocalPath("raw/Cabspotting/final_data_100meters", false);
                //= Utils.listFilesFromLocalPath("raw/Cabspotting/final_data_leq4", false);

                //= Utils.listFilesFromLocalPath("raw/CenceMeGPS/final_data", false);

                //= Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data", false);
                //= Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data_leq4", false);
                //= Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data_n=100", false);
                //= Utils.listFilesFromLocalPath("raw/DartmouthWiFi2/final_data", false);

                //= Utils.listFilesFromLocalPath("raw/IleSansFils/final_data", false);

        LinkedHashMap<Dataset, List<String>> dtsPPUserFilePathsMap = new LinkedHashMap<>();
        dtsPPUserFilePathsMap.put(Dataset.IleSansFils,
                Utils.listFilesFromLocalPath("raw/IleSansFils/final_data", false));
        dtsPPUserFilePathsMap.put(Dataset.CenceMeGPS,
                Utils.listFilesFromLocalPath("raw/CenceMeGPS/final_data", false));
        dtsPPUserFilePathsMap.put(Dataset.DartmouthWiFi,
                Utils.listFilesFromLocalPath("raw/DartmouthWiFi/final_data", false));
        dtsPPUserFilePathsMap.put(Dataset.Cabspotting,
                 Utils.listFilesFromLocalPath("raw/Cabspotting/final_data", false));

        long errorMarginThetaInSeconds
                //= 0;
                //= 60; // 1 minute
                //= 5 * 60;
                = 900; // 15 * 60
        System.out.println("Error margin theta is " + errorMarginThetaInSeconds / 60 + " minute(s)");
        int numPredictionsToPerform = 1000; //1000;
        System.out.println("Number of predictions to perform: " + numPredictionsToPerform);

        //generate the deltaSeconds list
        long[] deltaSecs = new long[]{
                        //0,
                        5 * 60,
                        15 * 60,
                        30 * 60,
                        60 * 60,
                        120 * 60,
                        240 * 60,
                        480 * 60
                        };


        ArrayList<NextPlaceConfiguration> npConfs = new ArrayList<>();
        for (int m = 3; m >= 1; m--)
        {
            NextPlaceConfiguration nextPlaceConfiguration = new NextPlaceConfiguration
                    (m, delayParameterV, DistanceMetric
                            //.ComparativeLoopingDistance,
                            //.ComparativeEuclidean,
                            .Euclidean,
                            NextPlaceConfiguration.NeighborhoodType.EPSILON_NEIGHBORHOOD
                    );
            npConfs.add(nextPlaceConfiguration);
        } // for

        for(Map.Entry<Dataset, List<String>> entry: dtsPPUserFilePathsMap.entrySet())
        for(NextPlaceConfiguration nextPlaceConfiguration : npConfs)
        {
            LinkedHashMap<Long, Double> deltaSecSumOfPredictionPrecisionsOfADataset = new LinkedHashMap<>();
            LinkedHashMap<Long, Integer> deltaSecSumNumberOfUserConsidered = new LinkedHashMap<>();
            for(long deltaSec : deltaSecs)
            {
                deltaSecSumOfPredictionPrecisionsOfADataset.put(deltaSec, 0.0);
                deltaSecSumNumberOfUserConsidered.put(deltaSec, 0);
            } // for


            System.out.println("\n---------------------------------------------------------------------------");
            System.out.println("Embedding parameter m = " + nextPlaceConfiguration.getEmbeddingParameterM());
            System.out.println("Distance metric: " + npConf.getDistanceMetric());

            long start = System.nanoTime();
            //for each sum up the results and average them by seeds.length
            for(int index = 0; index < seeds.length; index ++)
            {
                System.out.println("Run => " + (index + 1) + ": ");
            Tuple2<LinkedHashMap<Long, Integer>, LinkedHashMap<Long, Double>> tuple
                    = Utils
                    .distributedPredictionPrecisionOfADataset
                    //.predictionPrecisionOfADataset
                    (
                    entry.getKey(), //dts,
                    sc,
                    entry.getValue(), //ppUserFilePaths,
                    nextPlaceConfiguration, //npConf,
                    //0.5,
                    //0.5,
                    //0.7,
                    //0.3,
                    0.9,
                    0.1,
                    deltaSecs,
                    //0.9999999999,
                    //0.0000000001,
                    //0.95,
                    //0.05,
                    //0.9999,
                    //0.0001,
                    numPredictionsToPerform,
                    seeds[index], //5,
                    errorMarginThetaInSeconds
            );

            LinkedHashMap<Long, Double> deltaSecPredictionPrecisionOfADataset = tuple._2;
            for (Long deltaSec : deltaSecPredictionPrecisionOfADataset.keySet()) {
                double sum = deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec);
                sum += deltaSecPredictionPrecisionOfADataset.get(deltaSec);

                //sum up the prediction precisions for all seeds
                deltaSecSumOfPredictionPrecisionsOfADataset.put(deltaSec, sum);
            } // for


            LinkedHashMap<Long, Integer> deltaSecNumberOfUserConsidered = tuple._1;
            for (Long deltaSec : deltaSecNumberOfUserConsidered.keySet()) {
                int sum = deltaSecSumNumberOfUserConsidered.get(deltaSec);
                sum += deltaSecNumberOfUserConsidered.get(deltaSec);

                //sum up the prediction precisions for all seeds
                deltaSecSumNumberOfUserConsidered.put(deltaSec, sum);
            } // for
            } // for each seed
            long end = System.nanoTime();


            System.out.println("M = " + nextPlaceConfiguration.getEmbeddingParameterM()
                    +  ", average nano runtime over " + seeds.length + " seeds => " + ( (end-start) / seeds.length ) );

//        //average of all seeds
//        System.out.println("\n");
//        for(Long deltaSec : deltaSecSumOfPredictionPrecisionsOfADataset.keySet())
//            System.out.println("Prediction Precision of " + dts +  " at T + " + deltaSec / 60 + "min => "
//                    + String.format("%.2f",
//                    deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "%");

//        System.out.println("\n");
//        for(Long deltaSec : deltaSecSumOfPredictionPrecisionsOfADataset.keySet())
//        {
//            switch (deltaSec.intValue())
//            {
//                case 5 * 60: System.out.println("(" + 0 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 15 * 60: System.out.println("(" + 20 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 30 * 60: System.out.println("(" + 40 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 60 * 60: System.out.println("(" + 60 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 120 * 60: System.out.println("(" + 80 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 240 * 60: System.out.println("(" + 100 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//                case 480 * 60: System.out.println("(" + 120 + "," + String.format("%.2f",
//                        deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + ")"
//                        + " %" + deltaSec.intValue() / 60 + " minutes");
//                        break;
//            } // switch
//        } // for


            System.out.println("\n");
            for (Long deltaSec : deltaSecSumOfPredictionPrecisionsOfADataset.keySet()) {
                switch (deltaSec.intValue()) {
                    case 5 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 0 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 15 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 20 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 30 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 40 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 60 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 60 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 120 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 80 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 240 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 100 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                    case 480 * 60:
                        System.out.println((deltaSecSumNumberOfUserConsidered.get(deltaSec) / seeds.length)
                                + "," + 120 + "," + String.format("%.2f",
                                deltaSecSumOfPredictionPrecisionsOfADataset.get(deltaSec) / seeds.length) + "\\\\"
                                + " %" + deltaSec.intValue() / 60 + " minutes");
                        break;
                } // switch
            } // for

            System.out.println("---------------------------------------------------------------------------");
        }  // for each npConf

        // --- CALCULATION OF PREDICTION PRECISION IN A LOCAL MODE -----





//        System.out.println("Calculating Prediction Precision for " + dts + " ...");
//        LinkedHashMap<Long, Integer> deltaSecNumberOfUsersConsidered = new LinkedHashMap<>();
//        deltaSecNumberOfUsersConsidered.put(5 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(15 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(30 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(60 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(120 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(240 * 60L, 0);
//        deltaSecNumberOfUsersConsidered.put(480 * 60L, 0);
//        LinkedHashMap<Long, Double> deltaSecSumOfPP = new LinkedHashMap<>();
//        deltaSecSumOfPP.put(5 * 60L, 0.0);
//        deltaSecSumOfPP.put(15 * 60L, 0.0);
//        deltaSecSumOfPP.put(30 * 60L, 0.0);
//        deltaSecSumOfPP.put(60 * 60L, 0.0);
//        deltaSecSumOfPP.put(120 * 60L, 0.0);
//        deltaSecSumOfPP.put(240 * 60L, 0.0);
//        deltaSecSumOfPP.put(480 * 60L, 0.0);
//
//        for(String ppUserFilePath : ppUserFilePaths)
//        {
//            InputStream in = Utils.inputStreamFromLocalFilePath(ppUserFilePath);
//            ArrayList<Visit> significantVisits = Utils.visitsFromPPUserFileInputStream(in);
//
//            LinkedHashMap<Long, Double> deltaSecPredictionPrecisionMap
//                    = Utils.predictionPrecisionOfAUserAtRandomTimeT(npConf, significantVisits,
//                    //0.99999999,
//                    //0.00000001,
//                    0.9,
//                    0.1,
//                    1000,
//                    5,
//                    900);
//
//            for(Long deltaSec : deltaSecPredictionPrecisionMap.keySet())
//            {
//                //System.out.println("Prediction Precision at T + " + deltaSec / 60 + "min => "
//                //        + String.format("%.2f", deltaSecPredictionPrecisionMap.get(deltaSec) * 100) + "%");
//                double pp = deltaSecPredictionPrecisionMap.get(deltaSec);
//                double sum = deltaSecSumOfPP.get(deltaSec);
//                int numberOfUsersConsidered = deltaSecNumberOfUsersConsidered.get(deltaSec);
//                if(!Double.isNaN(pp))
//                {
//                    sum += pp;
//                    numberOfUsersConsidered ++;
//                } // if
//                deltaSecSumOfPP.put(deltaSec, sum);
//                deltaSecNumberOfUsersConsidered.put(deltaSec, numberOfUsersConsidered);
//            } // for
//
//            //do it for one user now
//            //break;
//        } // for
//
//        System.out.println("Number of users: " + ppUserFilePaths.size());
//        for(Long deltaSec : deltaSecNumberOfUsersConsidered.keySet())
//            System.out.println("deltaMinutes: " + deltaSec / 60 + " => number of users considered: "
//                    + deltaSecNumberOfUsersConsidered.get(deltaSec));
//        for(Long deltaSec : deltaSecSumOfPP.keySet())
//            System.out.println("Prediction Precision of " + dts +  " at T + " + deltaSec / 60 + "min => "
//                    + String.format("%.2f", deltaSecSumOfPP.get(deltaSec) * 100 / deltaSecNumberOfUsersConsidered.get(deltaSec)) + "%");
        //----- LOCAL TEST MODE -----








        long endTime = System.nanoTime();
        System.out.println("|| Overall application runtime: " + ( (endTime - startTime) / (1000000000 * 60D) ) + " minutes");
        System.out.println("||-----------------------------------------------------------------------------------||");
    } // main









    public static void NULLCheckForIleSansFils()
    {
        String rawFilesDirPath = "raw/IleSansFils/unprocessed";
        List<String> rawFilePaths = Utils.listFilesFromLocalPath(rawFilesDirPath, false);

        //array list to hold arrival time differences for NULL endTime visits
        ArrayList<Double> timeStampInDifferencesInHours = new ArrayList<>();

        for(String rawFilePath : rawFilePaths)
        {
            ArrayList<Visit> unprocessedVisits = new ArrayList<>(Preprocessing.sortedOriginalVisitsFromRawFile(rawFilePath, Dataset.IleSansFils));
            for(int index = 0; index < unprocessedVisits.size() - 1; index ++)
            {
                //residenceSeconds == 0 visits are NULL endTime visits
                if(unprocessedVisits.get(index).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                {
                    double hourDifference = Utils.noOfSecondsBetween(unprocessedVisits.get(index).getTimeSeriesInstant().getArrivalTime(),
                            unprocessedVisits.get(index + 1).getTimeSeriesInstant().getArrivalTime()) / 3600.0; // division by 3600.0 to make hours
                    timeStampInDifferencesInHours.add(hourDifference);
                } // if
            } // for
        } // for

        //sort in ascending order
        Collections.sort(timeStampInDifferencesInHours);

        for(int index = 0; index < timeStampInDifferencesInHours.size(); index ++)
            System.out.println("#" + (index  + 1) + ": " + timeStampInDifferencesInHours.get(index));

        LinkedHashMultimap<Integer, Double> hourNumbers = LinkedHashMultimap.create();
        for(Double hourDifference : timeStampInDifferencesInHours)
        {
            int hourDiff = (int) Math.ceil(hourDifference);
            hourNumbers.put(hourDiff, hourDifference);
        } // for

        for(Integer key : hourNumbers.keySet())
        {
            //System.out.println(key + " => " + hourNumbers.get(key).size());
            System.out.println(key + "," + hourNumbers.get(key).size());
        }

        //after the analysis, found out that chosing 8 hours of max interval is appropriate
    } // NULLCheckForIleSansFils


    public static void prePreProcessingForIleSansFils()
    {
        System.out.println("Prepreprocessing started...");

        //Pre-processing with respect to mixed user id files
        BufferedReader bReader = null;
        String rawFilePath = "raw/IleSansFils/connections_anonymised.csv";
        File rawFile = new File(rawFilePath);

        try
        {
            bReader = new BufferedReader(new FileReader(rawFile));
            //single line of the document read
            String singleLine = null;

            //multimap to hold userName (userID) and line pairs
            SortedSetMultimap<String, String> multimap = TreeMultimap.create(Ordering.natural(), new Comparator<String>() //MultimapBuilder.treeKeys().treeSetValues(new Comparator<String>()
            {
                @Override
                public int compare(String line1, String line2)
                {
                    String[] singleLineComponents1 = line1.split(",");
                    String[] singleLineComponents2 = line2.split(",");

                    //0 -> conn_id
                    //1 -> timestamp_in  -> arrival time in "yyyy-MM-dd HH:mm:ss" format
                    //2 -> node_id       -> location
                    //3 -> timestamp_out -> sometimes NULL, in "yyyy-MM-dd HH:mm:ss" format
                    //4 -> user_id       -> user name, may have a default value of ""
                    //5 -> user_mac
                    //6 -> incoming
                    //7 -> outgoing

                    //obtain arrival time, end time and residence seconds
                    String dateFormatPattern = "yyyy-MM-dd HH:mm:ss";
                    String timeStampIn1 = singleLineComponents1[1];
                    String timeStampIn2 = singleLineComponents2[1];

                    //obtain locationName1 and locationName2
                    String locationName1 = singleLineComponents1[2];
                    String locationName2 = singleLineComponents2[2];

                    //obtain conn_id1 and conn_id2
                    String conn_id1 = singleLineComponents1[0];
                    String conn_id2 = singleLineComponents2[0];


                    if(timeStampIn1.equalsIgnoreCase("NULL") || timeStampIn2.equalsIgnoreCase("NULL"))
                    {
                        throw new RuntimeException("timestamp_in is NULL");
                    } //if

                    //arrival time can be generated, at this point
                    try
                    {
                        Date arrivalTime1 = Utils.toDate(timeStampIn1, dateFormatPattern);
                        Date arrivalTime2 = Utils.toDate(timeStampIn2, dateFormatPattern);

                        //three-way compareTo, more effective;
                        //because, user can be connected to different
                        //access points (locations) at the same time;
                        //note-that compareTo in multi maps also defines equality
                        int result = arrivalTime1.compareTo(arrivalTime2);
                        if(result == 0)
                        {
                            result = locationName1.compareTo(locationName2);
                            if(result == 0)
                                result = conn_id1.compareTo(conn_id2);
                        } // if
                        return result;

                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }

                } // compare
            }); //.build();


            //go through all lines of the file
            while((singleLine = bReader.readLine()) != null)
            {
                //0 -> conn_id
                //1 -> timestamp_in  -> arrival time in "yyyy-MM-dd HH:mm:ss" format
                //2 -> node_id       -> location
                //3 -> timestamp_out -> sometimes NULL, in "yyyy-MM-dd HH:mm:ss" format
                //4 -> user_id       -> user name, may have a default value of ""
                //5 -> user_mac
                //6 -> incoming
                //7 -> outgoing

                //split with comma
                String[] singleLineComponents = singleLine.split(",");
                //user name
                String userID = singleLineComponents[4];
                if(userID.equals("")) throw new RuntimeException("User id is empty string");

                multimap.put(userID, singleLine);
            } // while


            //now go through all users and generate files
            for(String userName : multimap.keySet())
            {
                PrintWriter pWriter = null;

                try
                {
                    File newFile = new File(rawFile.getParent() + System.getProperty("file.separator") + userName + ".csv");
                    newFile.createNewFile();
                    pWriter = new PrintWriter(newFile);

                    for(String fileLine : multimap.get(userName))
                        pWriter.println(fileLine);
                }
                catch (IOException ioex)
                {
                    ioex.printStackTrace();
                }
                finally {
                    if(pWriter != null)
                        pWriter.close();
                }
            } // for

        } // try
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        finally
        {

            try
            {
                if (bReader != null)
                    bReader.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        } // finally


        System.out.println("Prepreprocessing finished...");
    } // prePreProcessingForIleSansFils



    //helper method to pre-preprocess 6-year version of Ile Sans Fils dataset
    public static void prepreprocessingForIleSansFils2()
    {
        System.out.println("Prepreprocessing started...");

        //Pre-processing with respect to mixed user id files
        BufferedReader bReader = null;
        String rawFilePath = "raw/IleSansFils2/isf_wifidog_anonymised_data_nogeo_2004-08-27_2010-03-07.csv";
        File rawFile = new File(rawFilePath);

        try
        {
            bReader = new BufferedReader(new FileReader(rawFile));
            //single line of the document read
            String singleLine = null;

            //multimap to hold userName (userID) and line pairs
            SortedSetMultimap<String, String> multimap = TreeMultimap.create(Ordering.natural(), new Comparator<String>() //MultimapBuilder.treeKeys().treeSetValues(new Comparator<String>()
            {
                @Override
                public int compare(String line1, String line2)
                {
                    String[] singleLineComponents1 = line1.split(",");
                    String[] singleLineComponents2 = line2.split(",");

                    //0 -> conn_id
                    //1 -> user_id       -> user name, may have a default value of ""
                    //2 -> node_id       -> location
                    //3 -> timestamp_in  -> arrival time in "yyyy-MM-dd HH:mm:ss" format
                    //4 -> timestamp_out -> sometimes NULL, in "yyyy-MM-dd HH:mm:ss" format

                    //obtain arrival time, end time and residence seconds
                    String dateFormatPattern = "yyyy-MM-dd HH:mm:ss";
                    String timeStampIn1 = singleLineComponents1[3];
                    String timeStampIn2 = singleLineComponents2[3];

                    //obtain locationName1 and locationName2
                    String locationName1 = singleLineComponents1[2];
                    String locationName2 = singleLineComponents2[2];

                    //obtain conn_id1 and conn_id2
                    String conn_id1 = singleLineComponents1[0];
                    String conn_id2 = singleLineComponents2[0];


                    if(timeStampIn1.equalsIgnoreCase("NULL") || timeStampIn2.equalsIgnoreCase("NULL"))
                    {
                        throw new RuntimeException("timestamp_in is NULL");
                    } //if

                    //arrival time can be generated, at this point
                    try
                    {
                        Date arrivalTime1 = Utils.toDate(timeStampIn1, dateFormatPattern);
                        Date arrivalTime2 = Utils.toDate(timeStampIn2, dateFormatPattern);

                        //three-way compareTo, more effective;
                        //because, user can be connected to different
                        //access points (locations) at the same time;
                        //note-that compareTo in multi maps also defines equality
                        int result = arrivalTime1.compareTo(arrivalTime2);
                        if(result == 0)
                        {
                            result = locationName1.compareTo(locationName2);
                            if(result == 0)
                                result = conn_id1.compareTo(conn_id2);
                        } // if
                        return result;

                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }

                } // compare
            }); //.build();


            //go through all lines of the file
            while((singleLine = bReader.readLine()) != null)
            {
                //0 -> conn_id
                //1 -> user_id       -> user name, may have a default value of ""
                //2 -> node_id       -> location
                //3 -> timestamp_in  -> arrival time in "yyyy-MM-dd HH:mm:ss" format
                //4 -> timestamp_out -> sometimes NULL, in "yyyy-MM-dd HH:mm:ss" format

                //split with comma
                String[] singleLineComponents = singleLine.split(",");
                //user name
                String userID = singleLineComponents[1];
                if(userID.equals("")) throw new RuntimeException("User id is empty string");

                multimap.put(userID, singleLine);
            } // while


            //now go through all users and generate files
            for(String userName : multimap.keySet())
            {
                PrintWriter pWriter = null;

                try
                {
                    File newFile = new File(rawFile.getParent() + System.getProperty("file.separator") + userName + ".csv");
                    newFile.createNewFile();
                    pWriter = new PrintWriter(newFile);

                    for(String fileLine : multimap.get(userName))
                        pWriter.println(fileLine);
                }
                catch (IOException ioex)
                {
                    ioex.printStackTrace();
                }
                finally {
                    if(pWriter != null)
                        pWriter.close();
                }
            } // for

        } // try
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        finally
        {

            try
            {
                if (bReader != null)
                    bReader.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        } // finally


        System.out.println("Prepreprocessing finished...");
    } // prepreprocessingForIleSansFils


    private static void prePreProcessingForDartmouthWiFi()
    {
        //Pre-processing with respect to AP locations
        //initially null, to be closed in finally block
        BufferedReader bReader = null;
        String userRawDataFilePath = "raw/DartmouthWiFi2/APlocations.csv";
        String userRawDataFilesDir = "raw/DartmouthWiFi2/data";

        try
        {
            //single line of the document read
            String singleLine = null;

            //file for APlocations.csv file
            File file = new File(userRawDataFilePath);
            String folderPath = file.getAbsoluteFile().getParent();
            System.out.println(folderPath);

            //initialize buffered reader
            bReader = new BufferedReader(new FileReader(file));

            //skip the first line which is the info line
            bReader.readLine();

            //int i = 0;

            //location name, location hash map
            HashMap<String, Location> nameLocationHashMap = new HashMap<String,Location>();

            //go through all the lines of the file except the skipped first line
            while ((singleLine = bReader.readLine()) != null)
            {
                //System.out.println(++i);
                //split string and create respective objects
                String[] singleLineComponents = singleLine.split(",");
                String locationName = singleLineComponents[0];
                double latitude = Double.parseDouble(singleLineComponents[1]);
                double longitude = Double.parseDouble(singleLineComponents[2]);

                //populate the hash map
                //it won't be possible to obtain location from hash set by its name;
                //therefore, hash map is appropriate
                nameLocationHashMap.put(locationName, new Location(locationName, latitude, longitude));
            } // while



            List<String> rawFilePaths = Utils.listFilesFromLocalPath(userRawDataFilesDir, false);
            for(String rawFilePath : rawFilePaths)
            {
                BufferedReader rawFileReader = null;
                PrintWriter pWriter = null;

                try
                {
                    File rawFile = new File(rawFilePath);
                    rawFileReader = new BufferedReader(new FileReader(rawFile));
                    File newFile = new File(folderPath + System.getProperty("file.separator") + rawFile.getName());
                    newFile.createNewFile();
                    pWriter = new PrintWriter(newFile);
                    String line = null;
                    StringBuilder sb = new StringBuilder("");

                    //read all the lines of the file
                    while((line = rawFileReader.readLine()) != null)
                    {
                        String[] lineComponents = line.split("\\t");
                        String unixTimeStampString = lineComponents[0];
                        String locationName = lineComponents[1];

                        //OFF is added to APlocations.csv, therefore below check is not necessary
                        //double latitude;
                        //double longitude;
                        //if(locationName.equals("OFF"))
                        //{
                        //    latitude = -1;
                        //    longitude = -1;
                        //} // if
                        //else
                        //{
                        //    Location location = nameLocationHashMap.get(locationName);
                        //    latitude = location.getLatitude();
                        //    longitude = location.getLongitude();
                        //} // else
                        //pWriter.println(unixTimeStampString + "\t" + locationName + "\t"
                        //        + latitude + "\t" + longitude);

                        Location location = nameLocationHashMap.get(locationName);

                        //ResBldg69AP9 is added to APlocations.csv, therefore below check is not necessary
                        //if(location == null)
                        //{
                        //    System.out.println("location = " + null + ", locationName = " + locationName);
                        //    pWriter.println(unixTimeStampString + "\t" + locationName + "\t"
                        //            + (-1) + "\t" + (-1));
                        //} // if
                        //else


                        //now populate the new file with components
                        pWriter.println(unixTimeStampString + "\t" + locationName + "\t"
                                + location.getLatitude() + "\t" + location.getLongitude());
                    } // while
                } // try
                catch (IOException ioex)
                {
                    ioex.printStackTrace();
                } // catch
                catch(Exception ex)
                {
                    ex.printStackTrace();
                } // catch
                finally
                {
                    try
                    {
                        if (rawFileReader != null)
                            rawFileReader.close();
                    } // try
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    } // catch

                    if(pWriter != null)
                        pWriter.close();
                }
            } // for

        } // try
        catch(IOException ex)
        {
            ex.printStackTrace();
        } // catch
        finally
        {
            try
            {
                if (bReader != null)
                    bReader.close();
            } // try
            catch(Exception e) {
                e.printStackTrace();
            } // catch
        } // finally
    } // prePreProcessingForDarthmouthWiFi


    //helper method to perform single machine NextPlace algorithm test
    private static void testSingleMachineNextPlace(String localDirPathOfCenceMeGPSPPDatasetFiles,
                                                   String localDirPathOfCabspottingPPDatasetFiles,
                                                   String localDirPathOfDartmouthWiFiPPDatasetFiles,
                                                   String localDirPathOfIleSansFilsPPDatasetFiles,
                                                   NextPlaceConfiguration npConf,
                                                   boolean calculatePEForEachFile,
                                                   double trainSplitFraction,
                                                   double testSplitFraction,
                                                   long timeTInDaySeconds,
                                                   long deltaTSeconds,
                                                   long errorMarginInSeconds)
    {
        System.out.println("\nTesting single-machine NextPlace algorithm on datasets");

        Utils.doNextPlaceAndWriteResults(Dataset.Cabspotting, localDirPathOfCabspottingPPDatasetFiles,
                false, npConf, calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");

        Utils.doNextPlaceAndWriteResults(Dataset.DartmouthWiFi, localDirPathOfDartmouthWiFiPPDatasetFiles,
                false, npConf, calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");

        Utils.doNextPlaceAndWriteResults(Dataset.IleSansFils, localDirPathOfIleSansFilsPPDatasetFiles,
                false, npConf, calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");
    } // testSingleMachineNextPlace


    //helper method to test distributed NextPlace algorithm
    private static void testDistributedNextPlace(JavaSparkContext sc,
                                                 String hdfsDirPathOfCenceMeGPSPPDatasetFiles,
                                                 String hdfsDirPathOfCabspottingPPDatasetFiles,
                                                 String hdfsDirPathOfDartmouthWiFiPPDatasetFiles,
                                                 String hdfsDirPathOfIleSansFilsPPDatasetFiles,
                                                 NextPlaceConfiguration npConf,
                                                 boolean calculatePEForEachFile,
                                                 double trainSplitFraction,
                                                 double testSplitFraction,
                                                 long timeTInDaySeconds,
                                                 long deltaTSeconds,
                                                 long errorMarginInSeconds)
    {
        System.out.println("\nTesting distributed NextPlace algorithm on datasets");

        Utils.doDistributedNextPlaceAndWriteResults(Dataset.CenceMeGPS, sc,
                 hdfsDirPathOfCenceMeGPSPPDatasetFiles, false, npConf,
                calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");

        Utils.doDistributedNextPlaceAndWriteResults(Dataset.Cabspotting, sc,
                 hdfsDirPathOfCabspottingPPDatasetFiles, false, npConf,
                calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");

        Utils.doDistributedNextPlaceAndWriteResults(Dataset.DartmouthWiFi, sc,
                hdfsDirPathOfDartmouthWiFiPPDatasetFiles, false, npConf,
                calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");

        Utils.doDistributedNextPlaceAndWriteResults(Dataset.IleSansFils, sc,
                hdfsDirPathOfIleSansFilsPPDatasetFiles, false, npConf,
                calculatePEForEachFile, trainSplitFraction, testSplitFraction,
                timeTInDaySeconds, deltaTSeconds, errorMarginInSeconds,
                "_NextPlace_Results");
    } // testDistributedNextPlace


    //helper method to test single-machine PE calculation of the datasets
    private static void testSingleMachinePECalculation(NextPlaceConfiguration npConf,
                                                       double trainSplitFraction, double testSplitFraction,
                                                       String localDirPathOfCenceMeGPSPPDatasetFiles,
                                                       String localDirPathOfCabspottingPPDatasetFiles,
                                                       String localDirPathOfDartmouthWiFiPPDatasetFiles,
                                                       String localDirPathOfIleSansFilsPPDatasetFiles)
    {
        System.out.println("\nTesting single-machine PE calculation of datasets");

        System.out.println("|| PE of the " + Dataset.CenceMeGPS + " in a single-machine mode is "
                            + Utils.peOfADataset(Dataset.CenceMeGPS, localDirPathOfCenceMeGPSPPDatasetFiles,
                            false, npConf, trainSplitFraction, testSplitFraction));

        System.out.println("|| PE of the " + Dataset.Cabspotting + " in a single-machine mode is "
                            + Utils.peOfADataset(Dataset.Cabspotting, localDirPathOfCabspottingPPDatasetFiles,
                            false, npConf, trainSplitFraction, testSplitFraction));

        System.out.println("|| PE of the " + Dataset.DartmouthWiFi + " in a single-machine mode is "
                + Utils.peOfADataset(Dataset.DartmouthWiFi, localDirPathOfDartmouthWiFiPPDatasetFiles,
                false, npConf, trainSplitFraction, testSplitFraction));

        System.out.println("|| PE of the " + Dataset.IleSansFils + " in a single-machine mode is "
                + Utils.peOfADataset(Dataset.IleSansFils, localDirPathOfIleSansFilsPPDatasetFiles,
                false, npConf, trainSplitFraction, testSplitFraction));
    } // testSingleMachinePECalculation


    //helper method to test distributed PE calculation of the datasets
    private static void testDistributedPECalculation(JavaSparkContext sc,
                                                     NextPlaceConfiguration npConf,
                                                     double trainSplitFraction, double testSplitFraction,
                                                     String hdfsDirPathOfCenceMeGPSPPDatasetFiles,
                                                     String hdfsDirPathOfCabspottingPPDatasetFiles,
                                                     String hdfsDirPathOfDartmouthWiFiPPDatasetFiles,
                                                     String hdfsDirPathOfIleSansFilsPPDatasetFiles)
    {
        System.out.println("\nTesting distributed PE calculation of datasets");

        System.out.println("|| PE of the " + Dataset.CenceMeGPS + " in a distributed mode is "
                + Utils.peOfADatasetDistributed(Dataset.CenceMeGPS, sc, hdfsDirPathOfCenceMeGPSPPDatasetFiles,
                                                    false, npConf, trainSplitFraction, testSplitFraction)._1);

        System.out.println("|| PE of the " + Dataset.Cabspotting + " in a distributed mode is "
                + Utils.peOfADatasetDistributed(Dataset.Cabspotting, sc, hdfsDirPathOfCabspottingPPDatasetFiles,
                                                    false, npConf, trainSplitFraction, testSplitFraction)._1);

        System.out.println("|| PE of the " + Dataset.DartmouthWiFi + " in a distributed mode is "
                + Utils.peOfADatasetDistributed(Dataset.DartmouthWiFi, sc, hdfsDirPathOfDartmouthWiFiPPDatasetFiles,
                false, npConf, trainSplitFraction, testSplitFraction)._1);

        System.out.println("|| PE of the " + Dataset.IleSansFils + " in a distributed mode is "
                + Utils.peOfADatasetDistributed(Dataset.IleSansFils, sc, hdfsDirPathOfIleSansFilsPPDatasetFiles,
                false, npConf, trainSplitFraction, testSplitFraction)._1);
    } // testDistributedPECalculation



    public static JavaSparkContext initializeSpark(String master, boolean isLocalMode,
                                                    boolean discardHadoopHomeDirSetup,
                                                    String hadoopHomeDir,
                                                    int noOfWorkers)
    {
        //INFO some parts of logging disabled
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //set hadoop home directory, in my case it is bin directory in C:\winutils
        if(!discardHadoopHomeDirSetup)
            System.setProperty("hadoop.home.dir", hadoopHomeDir); //"C:\\winutils\\");

        //get spark configuration, it will be different for local mode and cluster mode
        SparkConf configuration = new SparkConf() // EMPTY CONF
                .setAppName("Distributed NextPlace");


        //increase spark network timeout, its interval and executor heartbeat time out
        //spark job never starts if the heartbeat interval is greater than the network timeout
        configuration.set("spark.network.timeout", "31536000000ms");    // 1 year
        configuration.set("spark.network.timeoutInterval", "31536000000ms");
        configuration.set("spark.executor.heartbeatInterval", "31536000000ms");
        configuration.set("spark.executor.memory", "1g"); //1GB default

        if(!isLocalMode)
        {
            //FOR CLUSTER MODE
            configuration.setMaster(master);
            configuration.set("spark.executor.memory", "1g" /*"2g"*/); //1GB default
            //noOfWorkers workers
            configuration.set("spark.executor.instances", "" + noOfWorkers);
        } // if
        else
        {
            //FOR LOCAL MODE
            //noOfWorkers threads
            configuration.setMaster("local[" + noOfWorkers + "]");


            //local[*] uses as many threads as the number of processors
            //available to the Java virtual machine
            //(it uses Runtime.getRuntime.availableProcessors() to know the number).
            //configuration.setMaster("local[*]"); //maxFailures default 4


            //local[N, maxFailures] (called local-with-retries) with N being * or the number of threads to use
            // as explained above) and maxFailures:
            // Number of failures of any particular task before giving up on the job. The total number of
            // failures spread across different tasks will not cause the job to fail;
            // a particular task has to fail this number of attempts. Should be greater than or equal to 1.
            // Number of allowed retries = this value - 1.
            //DEFAULT VALUE IS 4.


            //without setting number of threads; will run only in one thread;
            //default parallelism will be 1 if parallelism is defined like this:
            //configuration.setMaster("local");
        } // else

        JavaSparkContext sc = new JavaSparkContext(configuration);

        //print spark version
        System.err.println("Spark version: " + sc.version());

        //sqlContext can be used to convert rdds to dataframe
        //SQLContext sqlContext = new SQLContext(sc);

        return sc;
    } // initializeSpark

    //helper method to avoid writing System.out.println() again again
    private static void println(Object object)
    {
        //with dynamic method binding toString() of casted
        //(which is casted to Object in method parameter) instance
        //will be called
        System.out.println(object);
    } // println





    //helper method to test the extraction of significant places from wifi logs
    private static List<String> testSignificantPlacesFromWiFiLogs(Dataset dataset, String localPathString, int visitFrequencyThreshold,
                                                                    NextPlaceConfiguration npConf,
                                                                    double trainSplitFraction, double testSplitFraction,
                                                                    int uniqueLocationThreshold,
                                                                    boolean listLocalFilesRecursively)
    {
        //will return file paths with 0 significant locations
        ArrayList<String> uniqueLocationThresholdSignLocFilePaths = new ArrayList<String>();

        //multi map to hold merged visits size => pp user file path pairs
        TreeMultimap<Integer, String> visitsSizePPUserFilePathMultiMap = TreeMultimap.create(Ordering.natural().reverse(),
                                            Ordering.natural());

        //list to hold file paths for which each file has PE >= 2.0
        //ArrayList<String> filePathsBiggerEqualTo2PE = new ArrayList<String>();

        //sum of PEs, initially 0
        double sumOfPEsOfUsers = 0;

        //list file from local path
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        int fileCount = 0;
        int fileCountWhichHasSmaller1PE = 0;
        int fileCountWhichHasSmallerZeroPointFivePE = 0;
        int fileCountWhichHas1PE = 0;
        int fileCountWhichHasBigger1PE = 0;
        int fileCountWhichHasBiggerEqualTo2PE = 0;
        int fileCountWhichHasSmaller2PE = 0;


        /*
        long total = 0;
        //for each user, do visit frequency threshold based significant location extraction
        for(String ppUserFilePath: ppUserFilePaths)
        {
            InputStream inputStream = Utils.inputStreamFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileInputStream(inputStream);

            //String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            //ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);
            total += mergedVisits.size();
        }
        System.out.println("Total number of visits before processing: " + total);
        System.exit(0);
        */


        HashSet<Location> globalUniqueLocations = new HashSet<Location>();


        //hash set to hold (global) unique significant locations off all users in the dataset
        HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();

        //HashSet<String> globalUniqueLocNames = new HashSet<>();

        long countOfAllSignificantLocationsOfUsers = 0;

        //count of all locations of user; which will be equal to count of all visits of users
        long countOfAllLocationsOfUsers = 0;


        //total number of visits
        long totalNumberOfVisits = 0;
        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = 0;

        //for each user, do visit frequency threshold based significant location extraction
        for(String ppUserFilePath: ppUserFilePaths)
        {
            //StringBuilder sb = new StringBuilder("");
            //sb.append("---------------------- " + Utils.fileNameFromPath(ppUserFilePath) + " ------------------------------------\n");
            System.out.println("---------------------- #" + (++fileCount) + ": " + Utils.fileNameFromPath(ppUserFilePath)
                    + " ---------------------------------");

            //obtain file contents and merged visits from file contents
            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);


            //update the multi map with the visits size and pp user file path
            visitsSizePPUserFilePathMultiMap.put(mergedVisits.size(), ppUserFilePath);



            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;


            //update count of all locations of users
            countOfAllLocationsOfUsers += mergedVisits.size();

            //obtain unique locations before pre-processing
            HashSet<Location> uniqueLocations = new HashSet<Location>();
            //HashSet<String> uniqueLocNames = new HashSet<>();
            for(Visit v : mergedVisits)
            {
                uniqueLocations.add(v.getLocation());
                //uniqueLocNames.add(v.getLocation().getName());
            } // for


            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + uniqueLocations.size());
            globalUniqueLocations.addAll(uniqueLocations);
            //uniqueLocations.clear(); uniqueLocations = null;
            //globalUniqueLocNames.addAll(uniqueLocNames);
            //uniqueLocNames.clear(); uniqueLocNames = null;



            //specific significant location extraction method for DartmouthWiFi dataset
            //significant unique locations
            HashSet<Location> significantUniqueLocations = uniqueLocations; //new HashSet<Location>();



            //frequency or count is O(1) in hash multi set
            HashMultiset<Location> locationHashMultiset = Utils.locationHashMultiset(mergedVisits);
            ArrayList<Visit> finalMergedVisits = mergedVisits; //new ArrayList<Visit>();
            //for each visit, check whether its location' frequency (count) is above or equal to threshold
//            for(Visit v : mergedVisits)
//            {
//                Location location = v.getLocation();
//                if(locationHashMultiset.count(location) >= (int) visitFrequencyThreshold)
//                {
//                    finalMergedVisits.add(v);
//                    significantUniqueLocations.add(location);
//                } // if
//            } // for



            System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
            System.out.println("Number of final merged visits = " + finalMergedVisits.size());



            //update the array list with file paths which have uniqueLocationThreshold significant locations
            if(significantUniqueLocations.size() == uniqueLocationThreshold) uniqueLocationThresholdSignLocFilePaths.add(ppUserFilePath);

            countOfAllSignificantLocationsOfUsers += significantUniqueLocations.size();


            //update the global unique significant locations hash set
            globalUniqueSignificantLocations.addAll(significantUniqueLocations);
            //update the number of total visits
            totalNumberOfVisits += finalMergedVisits.size();
            //update total residence seconds
            for(Visit v : finalMergedVisits)
            {
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing += v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //update total residence seconds of significant visits of this user
                totalResSecondsOfSignificantVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
            } // for
            significantUniqueLocations.clear(); significantUniqueLocations = null;


            //update total residence seconds of all visits before pre-processing
            //based on the visit histories created before pre-processing
            //create visit histories for all merged visits, even for locations which are visited only once
            Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits);
            for(VisitHistory vh : visitHistories)
            {
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += vh.getTotalResidenceSeconds();

                //update total residence seconds of all visits of this user
                totalResSecondsOfAllVisitsOfThisUser += vh.getTotalResidenceSeconds();
            } // for
            visitHistories.clear(); visitHistories = null;


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            if(Double.isNaN(proportionForThisUser)) System.out.println(Utils.fileNameFromPath(ppUserFilePath) + " is NaN");

            avgProportionOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;


            //move file writing to the helper method
            /*
            //write results to gaussian folder
            StringBuilder writableContentBuilder = new StringBuilder("");
            for(Visit v : finalMergedVisits)
            {
                String user = v.getUserName();
                double latitude = v.getLocation().getLatitude();
                double longitude = v.getLocation().getLongitude();
                //getTime() returns: the number of milliseconds since January 1, 1970,
                //00:00:00 GMT represented by the given date;
                long unixTimeStampInMilliseconds = v.getTimeSeriesInstant().getArrivalTime().getTime();
                long residenceTimeInSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //one line will be comma-separated list of values;
                //therefore, they will be easily splittable by String.split() method later
                writableContentBuilder.append(user + "," + latitude + "," + longitude + ","
                        + unixTimeStampInMilliseconds + "," + residenceTimeInSeconds + "\n");
            } // for
            //now write the local file system
            Utils.writeToLocalFileSystem(ppUserFilePath, "_gaussian", writableContentBuilder.toString());
            */



            //calculate PE of this user
            double peOfUserFile = Utils.peOfPPUserFile(npConf, finalMergedVisits, trainSplitFraction, testSplitFraction);
            //if(finalMergedVisits.isEmpty()) peOfUserFile = 1.0;


            //for eliminating 0 visit histories which do not have anything eps-neighborhood
            //if(peOfUserFile > 2.0 ||  Double.compare(peOfUserFile, 2.0) == 0)
            //    uniqueLocationThresholdSignLocFilePaths.add(ppUserFilePath);


            //in the paper, 70% of the dataset have predictability error < 1.0
            //check whether our methods do the same
            if(peOfUserFile < 1.0)
                ++ fileCountWhichHasSmaller1PE;
            else if(peOfUserFile > 1.0)
                ++ fileCountWhichHasBigger1PE;


            //detect number of files which has PE smaller than 0.5
            if(peOfUserFile < 0.5)
                ++ fileCountWhichHasSmallerZeroPointFivePE;


            //check which has bigger 2 or equal to PE and smaller to 2 PE
            if(peOfUserFile < 2.0)
                ++ fileCountWhichHasSmaller2PE;
            else if(peOfUserFile > 2.0 || Double.compare(peOfUserFile, 2.0) == 0)
            {
                ++fileCountWhichHasBiggerEqualTo2PE;
                //filePathsBiggerEqualTo2PE.add(ppUserFilePath);
            }


            //check how many users are not predictable
            if(Double.compare(peOfUserFile, 1.0) == 0)
                ++ fileCountWhichHas1PE;


            //update the sum
            sumOfPEsOfUsers += peOfUserFile;
            System.out.println("PE of file with name: " + Utils.fileNameFromPath(ppUserFilePath) + " => " + peOfUserFile);
            System.out.println("Current average PE of all files: " + sumOfPEsOfUsers / fileCount);
            System.out.println(String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 1.0");
            System.out.println(String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 0.5");
            System.out.println("-----------------------------------------------------------------------\n");


            //currently do it for one file
            //break;
        } // for each ppUserFilePath


        //now calculate average proportion of time spent by each user in significant places
        //by the division of number of users
        avgProportionOfTimeSpentByEachUserInSignificantPlaces = avgProportionOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size();


        //calculate PE_global which is the predictability error of the dataset
        double PE_global = sumOfPEsOfUsers / ppUserFilePaths.size();

        System.out.println("|| totalResSecondsOfAllVisitsOfUsersBeforePreprocessing => " + totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);
        System.out.println("|| totalResSecondsOfAllVisitsOfUsersAfterPreprocessing => " + totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);
        System.out.println("|| Total number of unique locations before pre-processing => " + globalUniqueLocations.size());
        //System.out.println("|| Total number of unique location names before pre-processing => " + globalUniqueLocNames.size());



        System.out.println("|| Number of users: " + ppUserFilePaths.size());
        System.out.println("|| Total number of (significant) visits: " + totalNumberOfVisits);
        System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());
        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                + countOfAllSignificantLocationsOfUsers
                / (double) ppUserFilePaths.size()
                ));

        System.out.println("|| Average number of visits per user: " + (int) (totalNumberOfVisits / (double) ppUserFilePaths.size()));
        System.out.println("|| Average residence time in a place D (seconds): "
                + (int) (
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                //totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                /// ( 1.0 * globalUniqueSignificantLocations.size() )
                / ( 1.0 * countOfAllSignificantLocationsOfUsers )
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


        List<String> regularlyActiveUserFilePaths = new ArrayList<>();
        int limit = 2043;
        int count = 0;
        for(Integer size : visitsSizePPUserFilePathMultiMap.keySet())
        {
            for(String regularlyActiveUserFilePath : visitsSizePPUserFilePathMultiMap.get(size))
            {
                if(count < limit)
                    regularlyActiveUserFilePaths.add(regularlyActiveUserFilePath);

                count ++;
            } // for
        } // for


        ppUserFilePaths.removeAll(regularlyActiveUserFilePaths);
        return ppUserFilePaths; //uniqueLocationThresholdSignLocFilePaths;
        //return filePathsBiggerEqualTo2PE;
    } // testSignificantPlacesFromWiFiLogs



    //overloaded version of test gaussians to check different threshold values
    private static Tuple2<Double, Tuple2<Double, Double>> testGaussians(//JavaSparkContext sc,
                                                      Dataset dataset, String localPathString,
                                                      Map<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap,
                                                      double thresholdT,
                                                      //double sigma,
                                                      //NextPlaceConfiguration npConf,
                                                      //double trainSplitFraction, double testSplitFraction,
                                                      boolean listLocalFilesRecursively)
    {
        //extract paths
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);

        //hash set to hold (global) unique significant locations off all users in the dataset
        //HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        long countOfAllSignificantLocationsOfUsers = 0;

        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = 0;



//        final HashSet<Location> allUniqueLocations = new HashSet<>();
//        userFileNameLocationWeightedCumulativeGaussianMap.forEach((s, locationDoubleMap) ->
//        {
//            allUniqueLocations.addAll(locationDoubleMap.keySet());
//        });
//        int numberOfAllUniqueLocations = allUniqueLocations.size();



        int fileCount = 0;
        //for each user, do gaussian pre-processing
        for(String ppUserFilePath: ppUserFilePaths)
        {
            System.out.println("---------------------- #" + (++fileCount) + ": " + Utils.fileNameFromPath(ppUserFilePath)
                    + " ---------------------------------");

            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);


            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;



            Map<Location, Double> locationAndWeightedCumulativeGaussianMap
                    = userFileNameLocationWeightedCumulativeGaussianMap.get(Utils.fileNameFromPath(ppUserFilePath));


            //print initial size of merged visits and unique locations
            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + locationAndWeightedCumulativeGaussianMap.keySet().size());



            //gaussianizedVisits will have visits in chronological order,
            //since we mergedVisits are in chronological order and we process them in that order'
            ArrayList<Visit> gaussianizedVisits = new ArrayList<Visit>();
            HashSet<Location> significantUniqueLocations = new HashSet<Location>();


            //ArrayList<Visit> outlierVisits = new ArrayList<>();

            double max = 0;
            double min = 1000000000;


            //for each visit generate gaussianized merged visits by eliminating visits with locations whose
            //aggregated weighted gaussian is below or equals 0.10
            //mergedVisits.forEach( v ->
            //{
            //
            //});

            for(Visit v : mergedVisits)
            {
                long thisVisitResSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //update for all users
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += thisVisitResSeconds;
                //update for only this user
                totalResSecondsOfAllVisitsOfThisUser += thisVisitResSeconds;


                Location location = v.getLocation();
                double aggregatedWeightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
                if(aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;
                if(aggregatedWeightedGaussian < min) min = aggregatedWeightedGaussian;

                if(aggregatedWeightedGaussian > thresholdT)
                {
                    //if (
                    significantUniqueLocations.add(location); //)
                    //System.out.println( //"#" + count + " Location: " + location
                    //+ "; aggregated weighted gaussian value => " +
                    //       aggregatedWeightedGaussian);

                    //multiple visits can be performed to the same location
                    //so, #gaussianized visits >= #significant (unique) locations
                    gaussianizedVisits.add(v);


                    //update for all users
                    totalResSecondsOfAllVisitsOfUsersAfterPreprocessing += thisVisitResSeconds;

                    //update residence seconds of significant visits
                    totalResSecondsOfSignificantVisitsOfThisUser += thisVisitResSeconds;
                } // if
            } // for
            mergedVisits.clear(); mergedVisits = null;


            //count of all significant locations of users
            countOfAllSignificantLocationsOfUsers += significantUniqueLocations.size();
            //update the global unique significant locations hash set
            //globalUniqueSignificantLocations.addAll(significantUniqueLocations);

            // at the end allUniqueLocations will only contain insignificant locations
            //allUniqueLocations.removeAll(significantUniqueLocations);


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            if(Double.isNaN(proportionForThisUser)) System.out.println(Utils.fileNameFromPath(ppUserFilePath) + " is NaN");

            avgProportionOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;


            System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
            System.out.println("Number of gaussianized visits = " + gaussianizedVisits.size());
            System.out.println("Min cumulative weighted gaussian value = " + min);
            System.out.println("Max cumulative weighted gaussian value = " + max);
            System.out.println("-----------------------------------------------------------------------\n");

            gaussianizedVisits.clear(); significantUniqueLocations.clear();
        } // for


        //now calculate average proportion of time spent by each user in significant places
        //by the division of number of users
        avgProportionOfTimeSpentByEachUserInSignificantPlaces = avgProportionOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size();


        //System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());
        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                //+ (numberOfAllUniqueLocations - allUniqueLocations.size())
                + countOfAllSignificantLocationsOfUsers
                        / (double) ppUserFilePaths.size()
                ));
        System.out.println("|| Average proportion of time spent by each user in significant places: "
                + String.format("%.2f", totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing)
                //+ String.format("%.2f", avgProportionOfTimeSpentByEachUserInSignificantPlaces)
                + " %");


        return new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
                countOfAllSignificantLocationsOfUsers / (double) ppUserFilePaths.size(),
                //avgProportionOfTimeSpentByEachUserInSignificantPlaces
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                ));
    } // testGaussians



    private static HashSet<Location> testGaussians(JavaSparkContext sc, Dataset dataset, String localPathString, double thresholdT, double sigma,
                                      NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction,
                                      boolean listLocalFilesRecursively)
    {
        //sum of PEs, initially 0
        double sumOfPEsOfUsers = 0;

        //TESTING GAUSSIANS
        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        int fileCount = 0;
        int fileCountWhichHasSmaller1PE = 0;
        int fileCountWhichHasSmallerZeroPointFivePE = 0;
        int fileCountWhichHas1PE = 0;
        int fileCountWhichHasBigger1PE = 0;
        int fileCountWhichHasBiggerEqualTo2PE = 0;
        int fileCountWhichHasSmaller2PE = 0;

        //max and min cumulative gaussian values of the dataset
        double maxCumulativeWeightedGaussianValueOfTheDataset = 0;
        double minCumulativeWeightedGaussianValueOfTheDataset = 1000000000;


        HashSet<Location> globalUniqueLocations = new HashSet<Location>();


        //hash set to hold (global) unique significant locations off all users in the dataset
        HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        long countOfAllSignificantLocationsOfUsers = 0;


        //count of all locations of user; which will be equal to count of all visits of users
        long countOfAllLocationsOfUsers = 0;


        //total number of visits
        long totalNumberOfVisits = 0;
        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = 0;

        //for each user, do gaussian pre-processing
        for(String ppUserFilePath: ppUserFilePaths)
        {
            //StringBuilder sb = new StringBuilder("");
            //sb.append("---------------------- " + Utils.fileNameFromPath(ppUserFilePath) + " ------------------------------------\n");
            System.out.println("---------------------- #" + (++fileCount) + ": " + Utils.fileNameFromPath(ppUserFilePath)
                                                            + " ---------------------------------");

            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);

            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);

            //Tuple2<Double, Double> maxLatitudeLongitudeTuple = Utils.maxLatitudeLongitudeTuple(mergedVisits);
            //Tuple2<Double, Double> minLatitudeLongitudeTuple = Utils.minLatitudeLongitudeTuple(mergedVisits);

            //sb.append("For user with file: " + Utils.fileNameFromPath(ppUserFilePath) + " => max(latitude, longitude) = "
            //        + "(" + maxLatitudeLongitudeTuple._1 + ", "
            //        + maxLatitudeLongitudeTuple._2 + ")\n");

            //sb.append("For user with file: " + Utils.fileNameFromPath(ppUserFilePath) + " => min(latitude, longitude) = "
            //        + "(" + minLatitudeLongitudeTuple._1 + ", "
            //        + minLatitudeLongitudeTuple._2 + ")\n");

            //System.out.println("For user with file: " + Utils.fileNameFromPath(ppUserFilePath) + " => max(latitude, longitude) = "
            //        + "(" + maxLatitudeLongitudeTuple._1 + ", "
            //        + maxLatitudeLongitudeTuple._2 + ")");

            //System.out.println("For user with file: " + Utils.fileNameFromPath(ppUserFilePath) + " => min(latitude, longitude) = "
            //        + "(" + minLatitudeLongitudeTuple._1 + ", "
            //        + minLatitudeLongitudeTuple._2 + ")");



            // METHOD 1 => distance < rangeInMetersToSmoothLocations based grouping of locations
            /*
            float rangeInMetersToSmoothLocations = 10.0f;

            HashSet<Location> uniqueLocations = new HashSet<Location>();
            for(Visit v : mergedVisits)
            {
                uniqueLocations.add(v.getLocation());
            } // for
            ArrayList<Location>  ul = new ArrayList<Location>(uniqueLocations);

            uniqueLocations.clear(); uniqueLocations = null;

            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + ul.size());
            //sb.append("Number of merged visits = " + mergedVisits.size() + "\n");
            //sb.append("Number of unique locations = " + ul.size() + "\n");

            HashMap<Integer, List<Location>> locationGroup = new HashMap<Integer, List<Location>>();
            for(int i = 0; i < ul.size(); i ++)
            {
                for (int j = 0; j < ul.size(); j++)
                {
                    float dist = ul.get(i).distanceTo(ul.get(j));

                    if(dist < rangeInMetersToSmoothLocations) //2.0f)
                    {
                        if(locationGroup.containsKey(i))
                        {
                            locationGroup.get(i).add(ul.get(j));
                        } // if
                        else
                        {
                            //list group will contain only unique locations, since ul contains only unique locations
                            List<Location> group = new ArrayList<Location>();
                            group.add(ul.get(j));
                            locationGroup.put(i, group);
                        } // else
                    } // if
                } // for
            } // for


            //for(Integer i : locationGroup.keySet())
            //{
            //    System.out.println("--------------- #" + (i + 1) + " Location Group ---------------");
            //    for(Location location : locationGroup.get(i))
            //    {
            //        System.out.println(location);
            //    } // for
            //    System.out.println("---------------------------------------------------------------");
            //} // for


            ul.clear(); ul = null;

            //now remove the groups with the same content
            //.values() will return a Collection<List<Location>> and hash set will remove duplicate elements;
            //List also has better hashCode implementation
            HashSet<List<Location>> locationGroupSet = new HashSet<List<Location>>(locationGroup.values());

            //System.out.println("locationGroup.values().size() = " + locationGroup.values().size());
            //System.out.println("locationGroupSet.size() = " + locationGroupSet.size());

            locationGroup.clear(); locationGroup = null;

            HashSet<Location> finalLocations = new HashSet<Location>();
            //hash set of averaged locations
            for(List<Location> locGroup : locationGroupSet)
            {
                double sumOfLatitudes = 0;
                double sumOfLongitudes = 0;

                //BigDecimal sumOfLatitudes = BigDecimal.ZERO;
                //BigDecimal sumOfLongitudes = BigDecimal.ZERO;

                //find average latitude
                for(Location location : locGroup)
                {
                    sumOfLatitudes += location.getLatitude();
                    sumOfLongitudes += location.getLongitude();
                    //sumOfLatitudes = sumOfLatitudes.add(BigDecimal.valueOf(location.getLatitude()));
                    //sumOfLongitudes = sumOfLongitudes.add(BigDecimal.valueOf(location.getLongitude()));
                } // for

                //find average
                double averageLat = //sumOfLatitudes.divide(BigDecimal.valueOf(locGroup.size()), BigDecimal.ROUND_UP).doubleValue();
                                    sumOfLatitudes / locGroup.size();
                double averageLng = //sumOfLongitudes.divide(BigDecimal.valueOf(locGroup.size()), BigDecimal.ROUND_UP).doubleValue();
                                    sumOfLongitudes / locGroup.size();

                //set the location name of the first location in a locGroup
                //to be location name of the averaged location
                finalLocations.add(new Location(locGroup.get(0).getName(), averageLat, averageLng));
            } // for


            locationGroupSet.clear(); locationGroupSet = null;


            ArrayList<Visit> modifiableMergedVisitList = new ArrayList<Visit>(mergedVisits);
            Iterator<Visit> visitIterator = modifiableMergedVisitList.iterator();
            ArrayList<Visit> gaussianizedVisits = new ArrayList<Visit>();
            while(visitIterator.hasNext())
            {
                //int oldSize = gaussianizedVisits.size();
                Visit v = visitIterator.next();

                for(Location location : finalLocations)
                {
                    float dist = v.getLocation().distanceTo(location);
                    if(dist < rangeInMetersToSmoothLocations) //2.0f)
                    {
                        Visit newVisit = new Visit(v.getUserName(), location, v.getTimeSeriesInstant());
                        gaussianizedVisits.add(newVisit);
                        visitIterator.remove();
                        break;
                    } // if
                } // for


            } // for

            modifiableMergedVisitList.clear(); modifiableMergedVisitList = null;
            //mergedVisits.clear(); mergedVisits = null;

            //sb.append("Number of unique final locations (after smoothing) = " + finalLocations.size() + "\n");
            System.out.println("Number of unique final locations (after smoothing) = " + finalLocations.size());

            finalLocations.clear(); finalLocations = null;

            //sb.append("Number of gaussianized merged visits = " + gaussianizedVisits.size() + "\n");
            System.out.println("Number of gaussianized merged visits = " + gaussianizedVisits.size());
            //System.out.println(sb.toString());
            */



            // METHOD 1.1 => check
            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;


            //update count of all locations of users
            countOfAllLocationsOfUsers += mergedVisits.size();


            ///*
            HashSet<Location> uniqueLocations = new HashSet<>();
            //ArrayList<Location> visitLocations = new ArrayList<>();
            for(Visit v : mergedVisits)
            {
                Location loc = v.getLocation();
                uniqueLocations.add(loc);
                //visitLocations.add(loc);
            } // for



            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + uniqueLocations.size());
            globalUniqueLocations.addAll(uniqueLocations);


            //Instances instances = Utils.getInstancesFrom(uniqueLocations);
            //System.out.println(instances);

            /*
            //The number of runs of k-means to perform
            int m_NumKMeansRuns = 10;
            //Number of threads to use for E and M steps
            int m_executionSlots = 1;

            // run k means a user-specified number of times and choose best solution
            SimpleKMeans bestKMeans = null;
            double bestSqE = Double.MAX_VALUE;
            for (int i = 0; i < m_NumKMeansRuns; i++)
            {
                SimpleKMeans sk = new SimpleKMeans();
                sk.setSeed(m_rr.nextInt());
                sk.setNumClusters(m_num_clusters);
                sk.setNumExecutionSlots(m_executionSlots);
                sk.setMaxIterations(1000); // default 500
                sk.setDistanceFunction(new GeographicalDistance());
                sk.setDisplayStdDevs(true);
                sk.setDoNotCheckCapabilities(true);
                sk.setDontReplaceMissingValues(true);

                sk.buildClusterer(instances);
                if (sk.getSquaredError() < bestSqE)
                {
                    bestSqE = sk.getSquaredError();
                    bestKMeans = sk;
                } // if
            } // for
            */


            /*
            //20 random seeds for 20 runs
            long[] seeds = new long[] {8L, 97L, 129L, 5632L, 34234L, 496217L, 2068041L, 38714554L, 9355793L, 819108L, 74598L, 6653L,
                    505L, 78L, 3L, 24L, 305L, 7787L, 12633L, 217812L};

            HashMap<Integer, Double> kSSEMap = new HashMap<>();
            for(int k = 2; k <= uniqueLocations.size(); k ++)
            {
                System.out.print("k = " + k + "; ");
                // run k means for defined seeds and choose best solution
                //SimpleKMeans localBestKMeans = null;
                //double localBestSqE = Double.MAX_VALUE;
                //for (int i = 0; i < seeds.length; i++)
                //{
                    try
                    {
                        SimpleKMeans sk = new SimpleKMeans();
                        //sk.setSeed((int) seeds[i]);
                        sk.setNumClusters(k);
                        sk.setNumExecutionSlots(1500); //Number of execution slots (default 1 - i.e. no parallelism)
                        sk.setMaxIterations(1000); // default 500
                        sk.setDistanceFunction(new GeographicalDistance());
                        sk.setDisplayStdDevs(true);
                        sk.setDoNotCheckCapabilities(true);
                        sk.setDontReplaceMissingValues(true);

                        sk.buildClusterer(instances);

                        //if (sk.getSquaredError() < localBestSqE)
                        //{
                        //    localBestSqE = sk.getSquaredError();
                        //    localBestKMeans = sk;
                        //} // if

                        kSSEMap.put(k, sk.getSquaredError());
                    } catch(Exception ex) { ex.printStackTrace(); }
                //} // for


                //kSSEMap.put(k, localBestSqE);
            } // for

            for(Map.Entry<Integer, Double> entry : kSSEMap.entrySet())
            {
                System.out.println(entry.getKey() + "," + entry.getValue());
            } // for
            */



//            KValid clusterer = new KValid();
//            try {
//                clusterer.setCascade(true); // Cascade test: Tries to find the best K given a minimum/maximum value
//                //clusterer.setSeed();
//                clusterer.setNumClusters(3); // default 2
//                clusterer.setMinimumK(3); //default 2
//                clusterer.setMaximumK(50); // default 10
//                clusterer.setShowGraph(true); // show average SILHOUETTE graph, or ELBOW_METHOD graph
//                clusterer.setInitializationMethod(new SelectedTag(KValid.KMEANS_PLUS_PLUS, SimpleKMeans.TAGS_SELECTION));
//                clusterer.setValidationMethod(new SelectedTag(KValid.SILHOUETTE_INDEX, KValid.VALIDATION_SELECTION));
//                clusterer.setNumExecutionSlots(1500); //Number of execution slots (default 1 - i.e. no parallelism)
//                clusterer.setMaxIterations(5000); // default 500
//                clusterer.setDistanceFunction(new GeographicalDistance());
//                clusterer.setDisplayStdDevs(true);
//                clusterer.setDoNotCheckCapabilities(true);
//                clusterer.setDontReplaceMissingValues(true);
//
//                clusterer.buildClusterer(instances);
//
//                System.out.println("\nNumber of clusters: " + clusterer.getClusters().length + " <=> " + clusterer.numberOfClusters());
//                System.out.println(clusterer); // to show the plot
//            }catch(Exception ex){ ex.printStackTrace(); }



//            HierarchicalClusterer clusterer = new HierarchicalClusterer();
//            try
//            {
//                clusterer.setDistanceFunction(new GeographicalDistance());
//
//                //static final int SINGLE = 0;
//                //static final int COMPLETE = 1;
//                //static final int AVERAGE = 2;
//                //static final int MEAN = 3;
//                //static final int CENTROID = 4;
//                //static final int WARD = 5;
//                //static final int ADJCOMPLETE = 6;
//                //static final int NEIGHBOR_JOINING = 7;
//
//                clusterer.setLinkType(new SelectedTag(0, HierarchicalClusterer.TAGS_LINK_TYPE)); //default SINGLE = 0
//                //If set to false, the distance between clusters is interpreted as the height of the node linking the clusters.
//                //This is appropriate for example for single link clustering.
//                //However, for neighbor joining, the distance is better interpreted as branch length. Set this flag to get the latter interpretation.
//                clusterer.setDistanceIsBranchLength(false);
//
//                //Flag to indicate whether the cluster should be print in Newick format.
//                //This can be useful for display in other programs.
//                //However, for large datasets a lot of text may be produced, which may not be a nuisance when the Newick format is not required.
//                clusterer.setPrintNewick(true); //default true
//
//
//                //build the clusterer
//                clusterer.buildClusterer(instances);
//
//
//                System.out.println("\nNumber of clusters: " + /*clusterer.getClusters().length +*/ " <=> " + clusterer.numberOfClusters());
//                System.out.println(clusterer); // to print summary
//            } catch(Exception ex) { ex.printStackTrace(); }


            /*
            EM clusterer = new EM();
            try
            {
                clusterer.setMaxIterations(200); //default 100
                clusterer.setNumFolds(10); // number of cv folds; default 10
                clusterer.setNumClusters(-1); // to find optimal k by cross validation
                clusterer.setNumKMeansRuns(10); // default 10, number of internal k-means runs to find best solution
                clusterer.setNumExecutionSlots(1500); //Number of execution slots (default 1 - i.e. no parallelism)
                clusterer.setMaxIterationsForInternalKMeans(1000); //default 500 in EM as well as in SimpleKMeans
                clusterer.setDistanceFunction(new GeographicalDistance()); //default Euclidean distance
                clusterer.buildClusterer(instances);

                System.out.println("Number of clusters: " + clusterer.getClusters().length + " <=> " + clusterer.numberOfClusters());
                System.out.println(clusterer);
            } // try
            catch(Exception ex)
            {
                ex.printStackTrace();
            } // catch
            */


//            Instances[] clusters = clusterer.getClusters();
//            for(int index = 0; index < clusters.length; index ++)
//            {
//                Instances cluster = clusters[index];
//                System.out.println("---------------------- Cluster: " + (index + 1) + "; #elements: "
//                                    + cluster.numInstances() + "------------------------------");
//                System.out.println(cluster);
//                System.out.println("----------------------- Summary String for Cluster: " + (index + 1) + "--------------------------------------");
//                System.out.println(instances.toSummaryString());
//                System.out.println("---------------------------------------------------------------------");
//                System.out.println("\n");
//
//
//                StringBuilder sb = new StringBuilder("");
//                for(Instance instance : cluster)
//                {
//                    double lat = instance.value(0);
//                    double lng = instance.value(1);
//                    sb.append(lat).append(",").append(lng).append("\n");
//                } // for
//
//                Utils.writeToLocalFileSystem(ppUserFilePath, "_cluster" + (index + 1), sb.toString());
//            } // for
//            System.out.println("finished");



//            System.out.println("Clustering started...");
//            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
//            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
//            //If these areas are located in different parts of the city,
//            //the following code will partition the events in different clusters by looking at each location.
//            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
//            // and we start clustering if there are at least three points close to each other.
//            double epsilon = 0.001; //100; // if haversine distance is used
//            int minPts = 3;
//            HashMap<Integer, HashSet<Location>> clusterMap
//                    = TestScala.gdbscan(uniqueLocations, ppUserFilePath, epsilon, minPts);
//
//            //get the clustered locations, clustered locations will generally be less than unique locations,
//            //outlier locations usually have one visit performed to them; we discard visits performed to outlier locations
//            HashSet<Location> locationsInClusters = new HashSet<>();
//            //map to hold location and its cluster id
//            HashMap<Location, Integer> locationClusterIDMap = new HashMap<>();
//
//            //get the the locations in all clusters and get location and cluster id map
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                locationsInClusters.addAll(cluster);
//
//                //now populate location and cluster id map
//                for(Location loc : cluster)
//                {
//                    locationClusterIDMap.put(loc, clusterID);
//                } // for
//            } // for
//
//            //now generate clustered visits with the help of clustered locations
//            //clustered visits be in chronological order as we process merged visits in chronological order
//            ArrayList<Visit> clusteredVisits = new ArrayList<>();
//            for(Visit v : mergedVisits)
//            {
//                if(locationsInClusters.contains(v.getLocation()))
//                    clusteredVisits.add(v);
//            } // for
//            System.out.println("Clustering ended...");
//
//
//            System.out.println("Number of (clustered) visits = " + clusteredVisits.size()); //mergedVisits.size());
//            System.out.println("Number of unique (clustered) locations = " + locationsInClusters.size()); //uniqueLocations.size());
//            globalUniqueLocations.addAll(locationsInClusters); //(uniqueLocations);


            //HashSet<Location> outliers = Utils.asymmetricDifference(uniqueLocations, locationsInClusters);
            //clusterMap.put(null, outliers);
            //System.out.println("Number of outliers: " + outliers.size());
            //for(Location outlier : outliers)
            //    System.out.println(outlier.toSimpleString());



            //if(true) continue; // discard other lines
            //if(true) break; // just consider one file




            //create visit histories
            Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits); //(clusteredVisits);
            //now find out total residence second of all visits and total residence seconds for each unique location
            long totalResSecondsOfAllVisitsOfThisUser_W = 0;

            //map to hold location and its total residence seconds; l -> w_l
            HashMap<Location, Long> locationAndTotalResSecondsOfThisUser_l_w_l_Map = new HashMap<Location, Long>();

            //now for each visit history, update total res seconds of all visits and the map;
            //number of visit histories is the same as number of unique locations
            for(VisitHistory vh : visitHistories)
            {
                //vh.getTotalResidenceSeconds() is w_l which is weight for the location of a visit history
                totalResSecondsOfAllVisitsOfThisUser_W += vh.getTotalResidenceSeconds();

                //now populate the map
                locationAndTotalResSecondsOfThisUser_l_w_l_Map.put(vh.getLocation(), vh.getTotalResidenceSeconds());

                //update total residence seconds of all visits of this user
                totalResSecondsOfAllVisitsOfThisUser += vh.getTotalResidenceSeconds();
            } // for
            visitHistories.clear(); visitHistories = null;



//            //location and weighted cumulative gaussian map, of the given muLocation with respect to other locations
//            HashMap<Location, Double> locationAndWeightedCumulativeGaussianMap = new HashMap<Location, Double>();
//
//            //for each unique location, find its weighted cumulative gaussian value with respect to other locations;
//            //we also calculate gaussian value of location with respect to itself
//            for(Location muLocation : uniqueLocations)
//            {
//                double weightedCumGaussian_F_mu_l_hat = 0;
//
//                //calculate F_mu_l_hat = sum{ol from uniqueLocations} F_mu_l(ol) * w_ol / W
//                for(Location otherLocation : uniqueLocations)
//                {
//                    long totalResSecondsOfOtherLocation_w_ol = locationAndTotalResSecondsOfThisUser_l_w_l_Map.get(otherLocation);
//                    double gaussian_F_mu_l_ol = Utils.gaussian(muLocation, otherLocation, 10); //F_mu_l(ol)
//                    weightedCumGaussian_F_mu_l_hat += gaussian_F_mu_l_ol
//                                            // multiplication by weight
//                                            * totalResSecondsOfOtherLocation_w_ol
//                                            / (1.0 * totalResSecondsOfAllVisitsOfThisUser_W);
//                } // for
//
//                //update the map
//                locationAndWeightedCumulativeGaussianMap.put(muLocation, weightedCumGaussian_F_mu_l_hat);
//            } // for



            int numUniqueLocations = uniqueLocations.size();
            int numPartitions = numUniqueLocations > 100000 ? numUniqueLocations / 100 :
                    (numUniqueLocations > 10000 ? numUniqueLocations / 10 : numUniqueLocations);


            Map<Location, Double> locationAndWeightedCumulativeGaussianMap
                =
                    // OLD 1. mergedVisits_to_clusteredVisits; totalResSecondsOfAllVisitsW
                    Utils.distributedWeightedCumulativeGaussian
                    (sc, //locationsInClusters.size() / 5, locationsInClusters,
                            numPartitions, uniqueLocations, //visitLocations,
                                locationAndTotalResSecondsOfThisUser_l_w_l_Map, totalResSecondsOfAllVisitsOfThisUser_W, sigma);

                    // NEW 2. mergedVisits_to_clusteredVisits; totalResSeconds in cluster with clusterID (W_clusterID)
                    //Utils.distributedWeightedCumulativeGaussian
                    //        (sc, locationsInClusters.size() / 5, locationsInClusters,
                    //        locationAndTotalResSecondsOfThisUser_l_w_l_Map, clusterMap, locationClusterIDMap, sigma);


            //clear memory
            locationAndTotalResSecondsOfThisUser_l_w_l_Map.clear(); locationAndTotalResSecondsOfThisUser_l_w_l_Map = null;
            
            

            // COMMENTED WRONG OLD METHOD to calculate generate locationAndWeightedCumulativeGaussianMap
//            HashMap<Location, Double> locationAndCumulativeGaussianMap = new HashMap<Location, Double>();
//            for(Location location : uniqueLocations)
//            {
//                locationAndCumulativeGaussianMap.put(location, 0.0);
//            } // for
//
//
//
//
//            //Save mutual gaussian value in a map, instead of calculating it again
//            //HashMap<Map.Entry<Location, Location>, Double> hmp = new HashMap<Map.Entry<Location, Location>, Double>();
//
//            //for each location, find its cumulative gaussian value with respect to other location
//            for(Location muLocation : locationAndCumulativeGaussianMap.keySet())
//            {
//                double cumGaussian = locationAndCumulativeGaussianMap.get(muLocation);
//
//                //method finds cumulative gaussian value with respect other locations
//                //therefore, remove muLocation from uniqueLocations, apply the method
//                //then add it again at the end of the loop
//                uniqueLocations.remove(muLocation);
//
//                for(Location otherLocation : uniqueLocations)
//                {
//                    //VERY SLOW
//                    //----NEW----
//                    //double mutualGaussian;
//                    //Map.Entry<Location, Location> mapEntry = new AbstractMap.SimpleEntry<Location, Location>(muLocation, otherLocation);
//
//                    //if(hmp.containsKey(mapEntry))
//                    //    mutualGaussian = hmp.get(mapEntry);
//                    //else
//                    //{
//                    //    mutualGaussian = Utils.gaussian(muLocation, otherLocation, 10);
//                    //    hmp.put(mapEntry, mutualGaussian);
//                    //} // else
//                    //cumGaussian += mutualGaussian;
//                    //----NEW----
//
//
//                    //----OLD----
//                    cumGaussian += Utils.gaussian(muLocation, otherLocation, 10);
//                    //----OLD----
//                } // for
//
//                locationAndCumulativeGaussianMap.put(muLocation, cumGaussian);
//                uniqueLocations.add(muLocation);
//            } // for
//
//
//            //above lines are replaced by this call to achieve spark parallel execution
//            //Map<Location, Double> locationAndCumulativeGaussianMap
//            //        = Utils.distributedCumulativeGaussian(sc, uniqueLocations.size(), uniqueLocations, sigma);
//
//
//
//            HashMap<Location, Double> locationAndWeightedCumulativeGaussianMap = new HashMap<Location, Double>();
//            for(Location location : uniqueLocations)
//            {
//                locationAndWeightedCumulativeGaussianMap.put(location, 0.0);
//            } // for
//
//
//            //create visit histories
//            Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits, 1);
//            for(VisitHistory vh : visitHistories)
//            {
//                for(Visit v : vh.getVisits())
//                {
//                    Location location = v.getLocation();
//                    double weightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
//                    double weight = v.getTimeSeriesInstant().getResidenceTimeInSeconds() / (vh.getTotalResidenceSeconds() * 1.0);
//                    weightedGaussian += locationAndCumulativeGaussianMap.get(location)
//                            *  weight;
//                    locationAndWeightedCumulativeGaussianMap.put(location, weightedGaussian);
//                } // for
//            } // for







//            HashMap<Integer, Double> clusterSumAggWeightedGaussianMap = new HashMap<>();
//            for(Integer clusterID : clusterMap.keySet())
//            {
//                HashSet<Location> cluster = clusterMap.get(clusterID);
//                double sumOfAggregatedWeightedGaussianOfLocationsInThisCluster = 0;
//                for(Location loc: cluster)
//                {
//                    sumOfAggregatedWeightedGaussianOfLocationsInThisCluster
//                            += locationAndWeightedCumulativeGaussianMap.get(loc);
//                } // for
//
//                //update cluster's sum of aggregated weighted gaussian of locations
//                clusterSumAggWeightedGaussianMap.put(clusterID, sumOfAggregatedWeightedGaussianOfLocationsInThisCluster);
//            } // for
//            clusterMap.clear(); clusterMap = null;




            // check
            //double maxPeakForThisUser = 0;
            //for(Location location : locationAndWeightedCumulativeGaussianMap.keySet())
            //{
            //    double aggregatedWeightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
            //    if(aggregatedWeightedGaussian > maxPeakForThisUser) maxPeakForThisUser = aggregatedWeightedGaussian;
            //} // for
            //thresholdT = 0.15 * maxPeakForThisUser; // choose threshold 15% of the highest peak
            // check



            //gaussianizedVisits will have visits in chronological order,
            //since we mergedVisits are in chronological order and we process them in that order'
            ArrayList<Visit> gaussianizedVisits = new ArrayList<Visit>();
            HashSet<Location> significantUniqueLocations = new HashSet<Location>();


            //ArrayList<Visit> outlierVisits = new ArrayList<>();

            double max = 0;
            double min = Double.POSITIVE_INFINITY;


            //for each visit generate gaussianized merged visits by eliminating visits with locations whose
            //aggregated weighted gaussian is below or equals 0.10
            for(Visit v : mergedVisits) //clusteredVisits)
            {
                Location location = v.getLocation();
                double aggregatedWeightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
                if(aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;
                if(aggregatedWeightedGaussian < min) min = aggregatedWeightedGaussian;


                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                if(aggregatedWeightedGaussian > thresholdT
                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0)
                {
                    //if (
                    significantUniqueLocations.add(location); //)
                    //System.out.println( //"#" + count + " Location: " + location
                    //+ "; aggregated weighted gaussian value => " +
                    //       aggregatedWeightedGaussian);

                    //multiple visits can be performed to the same location
                    //so, #gaussianized visits >= #significant (unique) locations
                    gaussianizedVisits.add(v);
                } // if
            } // for
            uniqueLocations.clear(); uniqueLocations = null;
            //locationsInClusters.clear(); locationsInClusters = null;
            locationAndWeightedCumulativeGaussianMap.clear(); locationAndWeightedCumulativeGaussianMap = null;




//            for(Visit v : clusteredVisits) //mergedVisits)
//            {
//                Location location = v.getLocation();
//                Integer clusterIDOfThisLocation = locationClusterIDMap.get(location);
//                //because of outliers clusterIDOfThisLocation can be null
//                //if(clusterIDOfThisLocation != null)
//                //{
//                    double sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster
//                            = clusterSumAggWeightedGaussianMap.get(clusterIDOfThisLocation);
//
//                    if(sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster > max)
//                        max = sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster;
//                    if(sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster < min)
//                        min = sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster;
//
//                    if(sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster > thresholdT)
//                    {
//                        significantUniqueLocations.add(location);
//                        //will contain chronologically ordered visits
//                        //since, we iterate mergedVisits in chronological order in this for loop
//                        gaussianizedVisits.add(v);
//                    } // if
//                //} // if
//
//                //else
//                //{
//                //    //double sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster
//                //    //        = clusterSumAggWeightedGaussianMap.get(clusterIDOfThisLocation);
//                //
//                //    //if(sumOfAggregatedWeightedGaussiansOfLocationsInThisCluster > thresholdT)
//                //    //{
//                //        //clusterID = null refers to outliers, therefore update outlier visits
//                //        outlierVisits.add(v);
//                //    //}
//                //} //else
//
//            } // for
//            clusterSumAggWeightedGaussianMap.clear(); clusterSumAggWeightedGaussianMap = null;
//            locationClusterIDMap.clear(); locationClusterIDMap = null;



            System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
            System.out.println("Number of gaussianized visits = " + gaussianizedVisits.size());
            System.out.println("Max cumulative weighted gaussian value = " + max);



//            Collection<VisitHistory> outlierVisitHistories = Utils.createVisitHistories(outlierVisits);
//            System.out.println("Number of outlier visits: " + outlierVisits.size());
//            System.out.println("Outlier visit histories; ");
//            int count = 0;
//            for(VisitHistory outlierVh : outlierVisitHistories)
//            {
//                System.out.println("Outlier visit history " + (++count) + ": ");
//                System.out.println(outlierVh.toStringWithDate());
//            }


            //if(true) break;
            //*/





            // METHOD 2 => applying 2-D gaussian distribution weighted by residence time in seconds
            /*
            //hash map will contain specific location and its aggregated weighted gaussian (peek value)
            //for each visit that is performed to this specific location;
            //weight will be provided by visit's time series instant's residence seconds -
            //which means for each visit the same peek value of this location will be multiplied by this visit's
            //residence seconds (weight) and summed up to have cumulative weighted gaussian value for this specific location
            HashMap<Location, Double> locationAndWeightedCumulativeGaussianMap = new HashMap<Location, Double>();
            for(Visit v : mergedVisits)
            {
                //every time will insert a new location to the map
                //the duplicate insertions will overwrite the previous;
                //at the end map's key set will only contain unique locations;
                //initially all locations will have 0.0 cumulative weighted gaussian (peek) value
                locationAndWeightedCumulativeGaussianMap.put(v.getLocation(), 0.0);
            } // for


            System.out.println("Number of merged visits = " + mergedVisits.size());
            System.out.println("Number of unique locations = " + locationAndWeightedCumulativeGaussianMap.size());



            //generate location and peek map in a distributed mode
            Map<Location, Double> locationAndPeekMap
                   = Utils.distributedPeekOfGaussian(sc, locationAndWeightedCumulativeGaussianMap.keySet(),
                    locationAndWeightedCumulativeGaussianMap.size(),
                    minLatitudeLongitudeTuple, maxLatitudeLongitudeTuple);

            //DOES NOT WORK => RESULTS IN 0 gaussianized merged visits
            //create visit histories
            //ArrayList<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits, 1);
            //for(VisitHistory vh : visitHistories)
            //{
            //    for(Visit v : vh.getVisits())
            //    {
            //        Location location = v.getLocation();
            //        double weightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
            //        double weight = v.getTimeSeriesInstant().getResidenceTimeInSeconds() /  (vh.getTotalResidenceSeconds() * 1.0);
            //        weightedGaussian += locationAndPeekMap.get(location)
            //                *  weight;
            //        locationAndWeightedCumulativeGaussianMap.put(location, weightedGaussian);
            //    } // for
            //} // for


            //for each visit's location populate locationAndWeightedCumulativeGaussianMap with cumulative weighted gaussian (peek) value
            for(Visit v : mergedVisits)
            {
                Location location = v.getLocation();
                double weightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
                weightedGaussian += locationAndPeekMap.get(location)
                        * v.getTimeSeriesInstant().getResidenceTimeInSeconds();
                locationAndWeightedCumulativeGaussianMap.put(location, weightedGaussian);
            } // for


            // gaussianizedVisits should have visits in chronological order
            // check this, otherwise use tree set
            //gaussianized merged visits
            ArrayList<Visit> gaussianizedVisits = new ArrayList<Visit>();
            HashSet<Location> significantUniqueLocations = new HashSet<Location>();

            double max = 0;

            //for each visit generate gaussianized merged visits by eliminating visits with locations whose
            //aggregated weighted gaussian is below or equals 0.10
            for(Visit v : mergedVisits)
            {
                Location location = v.getLocation();
                double aggregatedWeightedGaussian = locationAndWeightedCumulativeGaussianMap.get(location);
                if(aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;

                if(aggregatedWeightedGaussian > 0.10)
                {
                    //if (
                    significantUniqueLocations.add(location); //)
                        //System.out.println( //"#" + count + " Location: " + location
                                //+ "; aggregated weighted gaussian value => " +
                         //       aggregatedWeightedGaussian);

                    //multiple visits can be performed to the same location
                    //so, #gaussianized visits >= #significant (unique) locations
                    gaussianizedVisits.add(v);
                } // if
            } // for

            System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
            System.out.println("Number of gaussianized merged visits = " + gaussianizedVisits.size());
            System.out.println("Max cumulative weighted gaussian value = " + max);
            */



            // METHOD 3 => kernel density estimation
            /*
            //create visit histories and create weights which sum up 1.0
            ArrayList<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits, 1);
            //total residence seconds of all visits
            //will be used in weighting each visit location's gaussian with other locations
            long totalResidenceSecondsOfAllVisits = 0;
            for(VisitHistory vh : visitHistories)
            {
                totalResidenceSecondsOfAllVisits += vh.getTotalResidenceSeconds();
            } // for

            //generate locations and weights for merged visits in the same order
//            ArrayList<Location> locationsOfMergedVisits = new ArrayList<Location>();
//            ArrayList<Double> weightsOfLocationsOfMergedVisits = new ArrayList<Double>();
//            for(Visit v : mergedVisits)
//            {
//                locationsOfMergedVisits.add(v.getLocation());
//                weightsOfLocationsOfMergedVisits
//                        .add( (double) v.getTimeSeriesInstant().getResidenceTimeInSeconds()
//                        ); // / (totalResidenceSecondsOfAllVisits * 1.0) );
//            } // for

            //it is already a cumulative weighted KDE, because we have considered location of each visit and
            //each visit's residence time in seconds
            Map<Location, Double> uniqueLocationAndWeightedKDEMap
                    = Utils.distributedWeightedGaussianKDE(sc, mergedVisits); //, totalResidenceSecondsOfAllVisits);


            //this part is meaningless
//            HashMap<Location, Double> locationAndCumulativeWeightedGaussianKDEMap = new HashMap<Location, Double>();
//            for(Visit v : mergedVisits)
//            {
//                //every time will insert a new location to the map
//                //the duplicate insertions will overwrite the previous;
//                //at the end map's key set will only contain unique locations;
//                //initially all locations will have 0.0 cumulative weighted gaussian (peek) value
//                locationAndCumulativeWeightedGaussianKDEMap.put(v.getLocation(), 0.0);
//            } // for
//
//            System.out.println("Number of merged visits = " + mergedVisits.size());
//            System.out.println("Number of unique locations = " + locationAndCumulativeWeightedGaussianKDEMap.size());
//
//            //output the kernel density estimate of each visit's location
//            for(Visit v: mergedVisits)
//            {
//                //System.out.println("Weighted Gaussian KDE of this visit's location = "
//                //        + Utils.weightedGaussianKDE(v.getLocation(), locationsOfMergedVisits, weightsOfLocationsOfMergedVisits, 10));
//
//                Location location = v.getLocation();
//                double cumWeightedGaussianKDE = locationAndCumulativeWeightedGaussianKDEMap.get(location);
//                cumWeightedGaussianKDE
//                        += uniqueLocationAndWeightedKDEMap.get(location);
//                        //Utils.weightedGaussianKDE(location, locationsOfMergedVisits, weightsOfLocationsOfMergedVisits, 10);
//                locationAndCumulativeWeightedGaussianKDEMap.put(location, cumWeightedGaussianKDE);
//            } // for



            //gaussianizedVisits should have visits in chronological order
            //check this, otherwise use tree set
            //gaussianized merged visits
            ArrayList<Visit> gaussianizedVisits = new ArrayList<Visit>();
            HashSet<Location> significantUniqueLocations = new HashSet<Location>();

            double max = 0;

            //for each visit generate gaussianized merged visits by eliminating visits with locations whose
            //aggregated weighted gaussian is below or equals 0.10
            //int count = 0;
            for(Visit v : mergedVisits)
            {
                Location location = v.getLocation();
                double cumWeightedGaussianKDE = uniqueLocationAndWeightedKDEMap //locationAndCumulativeWeightedGaussianKDEMap
                        .get(location);
                if(cumWeightedGaussianKDE > max) max = cumWeightedGaussianKDE;

                if(cumWeightedGaussianKDE > 0.10)
                {
                    //if (
                    significantUniqueLocations.add(location); // )
                    //{
                    //    System.out.println("#" + (++count )+ " Location: " + location
                    //            + "; cum weighted gaussian KDE value => " +
                    //            cumWeightedGaussianKDE);
                    //}

                    //multiple visits can be performed to the same location
                    //so, #gaussianized visits >= #significant (unique) locations
                    gaussianizedVisits.add(v);
                } // if
            } // for

            System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
            System.out.println("Number of gaussianized merged visits = " + gaussianizedVisits.size());
            System.out.println("Max cumulative weighted gaussian KDE value = " + max);
            */



            //System.exit(0);


            //update max and min cumulative weighted gaussian values of the dataset
            if(max > maxCumulativeWeightedGaussianValueOfTheDataset) maxCumulativeWeightedGaussianValueOfTheDataset = max;
            if(min < minCumulativeWeightedGaussianValueOfTheDataset) minCumulativeWeightedGaussianValueOfTheDataset = min;


            countOfAllSignificantLocationsOfUsers += significantUniqueLocations.size();


            //update the global unique significant locations hash set
            globalUniqueSignificantLocations.addAll(significantUniqueLocations);
            //update the number of total visits
            totalNumberOfVisits += gaussianizedVisits.size();
            //update total residence seconds
            for(Visit v : gaussianizedVisits)
            {
                totalResSecondsOfAllVisitsOfUsersAfterPreprocessing += v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //update total residence seconds of significant visits of this user
                totalResSecondsOfSignificantVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
            } // for


            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            if(Double.isNaN(proportionForThisUser)) System.out.println(Utils.fileNameFromPath(ppUserFilePath) + " is NaN");

            avgProportionOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;



//            //update total residence seconds of all visits before pre-processing
//            //user the visit histories created before pre-processing occurs
//            for(VisitHistory vh : visitHistories)
//            {
//                totalResSecondsOfAllVisitsBeforePreprocessing += vh.getTotalResidenceSeconds();
//            } // for
            totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += totalResSecondsOfAllVisitsOfThisUser_W;




            //move file writing to the helper method
            /*
            //write results to gaussian folder
            StringBuilder writableContentBuilder = new StringBuilder("");
            for(Visit v : gaussianizedVisits)
            {
                String user = v.getUserName();
                double latitude = v.getLocation().getLatitude();
                double longitude = v.getLocation().getLongitude();
                //getTime() returns: the number of milliseconds since January 1, 1970,
                //00:00:00 GMT represented by the given date;
                long unixTimeStampInMilliseconds = v.getTimeSeriesInstant().getArrivalTime().getTime();
                long residenceTimeInSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //one line will be comma-separated list of values;
                //therefore, they will be easily splittable by String.split() method later
                writableContentBuilder.append(user + "," + latitude + "," + longitude + ","
                        + unixTimeStampInMilliseconds + "," + residenceTimeInSeconds + "\n");
            } // for
            //now write the local file system
            Utils.writeToLocalFileSystem(ppUserFilePath, "_gaussian", writableContentBuilder.toString());
            */


            //calculate PE of this user
            double peOfUserFile = Utils.peOfPPUserFile(npConf, gaussianizedVisits, trainSplitFraction, testSplitFraction);

            //in the paper, 70% of the dataset have predictability error < 1.0
            //check whether our methods do the same
            if(peOfUserFile < 1.0)
                ++ fileCountWhichHasSmaller1PE;
            else if(peOfUserFile > 1.0)
                ++ fileCountWhichHasBigger1PE;


            //detect number of files which has PE smaller than 0.5
            if(peOfUserFile < 0.5)
                ++ fileCountWhichHasSmallerZeroPointFivePE;


            //check which has bigger 2 or equal to PE and smaller to 2 PE
            if(peOfUserFile < 2.0)
                ++ fileCountWhichHasSmaller2PE;
            else if(peOfUserFile > 2.0 || Double.compare(peOfUserFile, 2.0) == 0)
            {
                ++fileCountWhichHasBiggerEqualTo2PE;
                //filePathsBiggerEqualTo2PE.add(ppUserFilePath);
            }


            //check how many users are not predictable
            if(Double.compare(peOfUserFile, 1.0) == 0)
                ++ fileCountWhichHas1PE;


            //update the sum
            sumOfPEsOfUsers += peOfUserFile;
            System.out.println("PE of file with name: " + Utils.fileNameFromPath(ppUserFilePath) + " => " + peOfUserFile);
            System.out.println("Current average PE of all files: " + sumOfPEsOfUsers / fileCount);
            System.out.println(String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 1.0");
            System.out.println(String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
                    + " % of the current processed files has PE < 0.5");



            //file writing for each user
            /*
            StringBuilder signLocStringBuilder = new StringBuilder("");
            //System.out.println("Significant locations: ");
            for(Location location : significantUniqueLocations)
            {
                double latitude = location.getLatitude();
                double longitude = location.getLongitude();
                //System.out.println(latitude + "," + longitude);
                signLocStringBuilder.append(latitude).append(",").append(longitude).append("\n");
            } // for
            Utils.writeToLocalFileSystem(ppUserFilePath, "_15PercentOfTheHighestPeak_", "",
                                                ".csv", signLocStringBuilder.toString());
            */



            //clear memory
            significantUniqueLocations.clear(); significantUniqueLocations = null;
            System.out.println("-----------------------------------------------------------------------\n");

            //Utils.distributedPeekOfGaussian(sc, mergedVisits, mergedVisits.size(), minLatitudeLongitudeTuple, maxLatitudeLongitudeTuple);



            //currently do it for one file
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
        //System.out.println("|| Total number of unique location names before pre-processing => " + globalUniqueLocNames.size());



        System.out.println("|| Number of users: " + ppUserFilePaths.size());
        System.out.println("|| Total number of (significant) visits: " + totalNumberOfVisits);
        System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());
        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                + countOfAllSignificantLocationsOfUsers
                        / (double) ppUserFilePaths.size()
        ));

        System.out.println("|| Average number of visits per user: " + (int) (totalNumberOfVisits / (double) ppUserFilePaths.size()));
        System.out.println("|| Average residence time in a place D (seconds): "
                + (int) (
                //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
                /// ( 1.0 * globalUniqueSignificantLocations.size() )
                /// ( 1.0 * countOfAllSignificantLocationsOfUsers )
                /// ( 1.0 * globalUniqueLocations.size())
                / (1.0 * countOfAllLocationsOfUsers)
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

        return globalUniqueSignificantLocations;
    } // testGaussians




    public static Tuple2<Double, Tuple2<Double, Double>> testClusteringOnCenceMeGPSDataset(//JavaSparkContext sc,
                                                                                            Dataset dataset,
                                                                                            List<String> ppUserFilePaths,
                                                                                            //String localPathString,
                                                                                            double thresholdT,
                                                                                            HashMap<String, List<Visit>> userFileNameMergedVisitsMap,
                                                                                            HashMap<String, HashSet<Location>> userFileNameUniqueGPSPointsMap,
                                                                                            HashMap<String, Long> userFileNameTotalResSecondsOfAllVisitsMap,
                                                                                            HashMap<String, Map<Location, Double>> userFileNameLocationWeightedCumulativeGaussianMap
                                                                                            //double sigma,
                                                                                            //NextPlaceConfiguration npConf,
                                                                                            //double trainSplitFraction, double testSplitFraction,
                                                                                            //boolean listLocalFilesRecursively
                                                                                           )
    {
        //sum of PEs, initially 0
        //double sumOfPEsOfUsers = 0;

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


        //int fileCount = 0;
        //int fileCountWhichHasSmaller1PE = 0;
        //int fileCountWhichHasSmallerZeroPointFivePE = 0;
        //int fileCountWhichHas1PE = 0;
        //int fileCountWhichHasBigger1PE = 0;
        //int fileCountWhichHasBiggerEqualTo2PE = 0;
        //int fileCountWhichHasSmaller2PE = 0;

        //max and min cumulative gaussian values of the dataset
        //double maxCumulativeWeightedGaussianValueOfTheDataset = 0;
        //double minCumulativeWeightedGaussianValueOfTheDataset = Double.POSITIVE_INFINITY;


        //HashSet<Location> globalUniqueLocations = new HashSet<Location>();


        //hash set to hold (global) unique significant locations off all users in the dataset
        //HashSet<Location> globalUniqueSignificantLocations = new HashSet<Location>();


        long countOfAllSignificantLocationsOfUsers = 0;


        //count of all locations of user; which will be equal to count of all visits of users
        //long countOfAllLocationsOfUsers = 0;


        //total number of visits
        //long totalNumberOfSignificantVisits = 0;
        //total residence seconds after pre-processing
        long totalResSecondsOfAllVisitsOfUsersAfterPreprocessing = 0;
        //total residence seconds before pre-processing
        long totalResSecondsOfAllVisitsOfUsersBeforePreprocessing = 0;

        //double average proportion of time spent by each user in significant places
        double avgProportionOfTimeSpentByEachUserInSignificantPlaces = 0;

        //for each user, do gaussian pre-processing
        for(String ppUserFilePath: ppUserFilePaths)
        {
            String keyFileName = Utils.fileNameFromPath(ppUserFilePath);
            //System.out.println("---------------------- #" + (++fileCount) + ": " + Utils.fileNameFromPath(ppUserFilePath)
            //        + " ---------------------------------");

            //String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            //ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);
            List<Visit> mergedVisits = userFileNameMergedVisitsMap.get(keyFileName);



            //total residence seconds of all visits of this user
            long totalResSecondsOfAllVisitsOfThisUser = 0;

            //total residence seconds of significant visits (visits performed to significant locations) of this user
            long totalResSecondsOfSignificantVisitsOfThisUser = 0;


            //update count of all locations of users
            //countOfAllLocationsOfUsers += mergedVisits.size();


            /*
            HashSet<Location> uniqueGPSPoints = new HashSet<Location>();
            for(Visit v : mergedVisits)
            {
                uniqueGPSPoints.add(v.getLocation());

                //update total residence seconds of all visits of this user
                totalResSecondsOfAllVisitsOfThisUser += v.getTimeSeriesInstant().getResidenceTimeInSeconds();


                //update total residence of all visits of all users before preprocessing
                totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += v.getTimeSeriesInstant().getResidenceTimeInSeconds();
            } // for
            */
            HashSet<Location> uniqueGPSPoints = userFileNameUniqueGPSPointsMap.get(keyFileName);
            totalResSecondsOfAllVisitsOfThisUser = userFileNameTotalResSecondsOfAllVisitsMap.get(keyFileName);
            totalResSecondsOfAllVisitsOfUsersBeforePreprocessing += totalResSecondsOfAllVisitsOfThisUser;




            //System.out.println("Number of merged visits = " + mergedVisits.size());
            //System.out.println("Number of unique locations = " + uniqueGPSPoints.size());
            //globalUniqueLocations.addAll(uniqueGPSPoints);



            //obtain \hat_{F_\mu} of each user
            Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                    = userFileNameLocationWeightedCumulativeGaussianMap.get(keyFileName);




            //significant GPS points which pass threshold T
            HashSet<Location> aboveThresholdGPSPointsOfThisUser = new HashSet<Location>();


            //min and max weighted cumulative gaussian values for this user
            //double max = 0;
            //double min = Double.POSITIVE_INFINITY;



            //for each unique location, check whether its \hat_{F_\mu} passes the threshold
            for(Location uniqueGPSPoint : uniqueGPSPoints)
            {
                double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(uniqueGPSPoint);
                //if(aggregatedWeightedGaussian > max) max = aggregatedWeightedGaussian;
                //if(aggregatedWeightedGaussian < min) min = aggregatedWeightedGaussian;


                //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                if(aggregatedWeightedGaussian > thresholdT
                        || Double.compare(aggregatedWeightedGaussian, thresholdT) == 0)
                {
                    aboveThresholdGPSPointsOfThisUser.add(uniqueGPSPoint);
                } // if
            } // for
            //uniqueGPSPoints.clear(); uniqueGPSPoints = null;
            //gpsPointAndWeightedCumulativeGaussianMap.clear(); gpsPointAndWeightedCumulativeGaussianMap = null;






            //System.out.println("\nClustering started...");
            //Suppose that a given user frequently visits three areas in a city—one for drinks and parties,
            //another for cozy and relaxing coffee breaks, and a yet another for dinners with friends.
            //If these areas are located in different parts of the city,
            //the following code will partition the events in different clusters by looking at each location.
            //In this code, we look for events close in proximity, in the range of 100 meters (about 0.001 degrees),
            // and we start clustering if there are at least three points close to each other.
            double epsilon = /*0.001;*/ 100; // if haversine distance is used
            int minPts = 3; // default 3

            //use tree map to preserve natural ordering of cluster ids
            TreeMap<Integer, HashSet<Location>> clusterMap
                    = null; //TestScala.gdbscan(aboveThresholdGPSPointsOfThisUser, ppUserFilePath, epsilon, minPts);

            //get the clustered gps points, clustered gps points will generally be less than aboveThresholdGPSPointsOfThisUser
            HashSet<Location> gpsPointsInClusters = new HashSet<>();
            //map to hold gps point and its cluster id
            HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();

            Integer lastClusterID = null;
            //get the the gps points in all clusters and update gps point and cluster id map
            for(Integer clusterID : clusterMap.keySet())
            {
                HashSet<Location> cluster = clusterMap.get(clusterID);
                gpsPointsInClusters.addAll(cluster);

                //now populate location and cluster id map
                for(Location gpsPoint : cluster)
                {
                    gpsPointClusterIDMap.put(gpsPoint, clusterID);
                } // for


                //update lastClusterID,
                //at the end of the loop it will get "last cluster ID" of the clusterMap or null if no clusters are found
                lastClusterID = clusterID;
            } // for


            //add outlierGPSPoints to be in their single point clusters
            HashSet<Location> outlierGPSPoints = Utils.asymmetricDifference(aboveThresholdGPSPointsOfThisUser, gpsPointsInClusters);
            //System.out.println("Number of outlier GPS points: " + outlierGPSPoints.size());
            for(Location outlierGPSPoint : outlierGPSPoints)
            {
                HashSet<Location> thisOutlierGPSPointCluster = new HashSet<>();
                thisOutlierGPSPointCluster.add(outlierGPSPoint);
                //System.out.println("lastClusterID = " + lastClusterID);
                if(lastClusterID == null) lastClusterID = 0;
                clusterMap.put(++lastClusterID, thisOutlierGPSPointCluster);

                //also update gps point and cluster id map
                gpsPointClusterIDMap.put(outlierGPSPoint, lastClusterID);
            } // for
            aboveThresholdGPSPointsOfThisUser.clear(); aboveThresholdGPSPointsOfThisUser = null;
            gpsPointsInClusters.clear(); gpsPointsInClusters = null;
            outlierGPSPoints.clear(); outlierGPSPoints = null;
            //System.out.println("Clustering ended...\n");





            //for each cluster compute its average location and store it in a map
            HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();
            for(Integer clusterID : clusterMap.keySet())
            {
                HashSet<Location> cluster = clusterMap.get(clusterID);
                Location averageLocationForThisCluster = Utils.averageLocation(cluster);
                clusterAverageLocationMap.put(clusterID, averageLocationForThisCluster);
            } // for
            clusterMap.clear(); clusterMap = null;



            //list to store significant visits
            ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

            //for each visit, if visit is significant, then reroute it its corresponding significant location
            for(Visit v : mergedVisits)
            {
                Location location = v.getLocation();
                //check whether this visit's location is within aboveThresholdGPSPoints,
                //where each gps point in aboveThresholdGPSPoints is contained in gpsPointClusterIDMap as a key
                if(gpsPointClusterIDMap.containsKey(location))
                {
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
            gpsPointClusterIDMap.clear(); gpsPointClusterIDMap = null;




            //update total proportion of time spent by users by a proportion of this user;
            //at the end of for loop we will divide this number by number of users
            double proportionForThisUser = (totalResSecondsOfSignificantVisitsOfThisUser * 100.0) / totalResSecondsOfAllVisitsOfThisUser;
            //if(Double.isNaN(proportionForThisUser)) System.out.println(keyFileName + " is NaN");

            avgProportionOfTimeSpentByEachUserInSignificantPlaces
                    += Double.isNaN(proportionForThisUser) ? 0 : proportionForThisUser;




            //update max and min cumulative weighted gaussian values of the dataset
            //if(max > maxCumulativeWeightedGaussianValueOfTheDataset) maxCumulativeWeightedGaussianValueOfTheDataset = max;
            //if(min < minCumulativeWeightedGaussianValueOfTheDataset) minCumulativeWeightedGaussianValueOfTheDataset = min;



            //update sum of count of all significant locations of each user
            countOfAllSignificantLocationsOfUsers += clusterAverageLocationMap.keySet().size();

            //update global unique significant locations hash set
            //globalUniqueSignificantLocations.addAll(clusterAverageLocationMap.values());


            //clear memory
            clusterAverageLocationMap.clear(); clusterAverageLocationMap = null;


            //total number of signficant visits of this user
            //totalNumberOfSignificantVisits += gaussianizedVisits.size();


            //System.out.println("Number of significant locations = " + clusterAverageLocationMap.keySet().size());
            //System.out.println("Number of gaussianized visits = " + gaussianizedVisits.size());
            //System.out.println("Min cumulative weighted gaussian value = " + min);
            //System.out.println("Max cumulative weighted gaussian value = " + max);





//            //calculate PE of this user
//            double peOfUserFile = Utils.peOfPPUserFile(npConf, gaussianizedVisits, trainSplitFraction, testSplitFraction);
//
//            //in the paper, 70% of the dataset have predictability error < 1.0
//            //check whether our methods do the same
//            if(peOfUserFile < 1.0)
//                ++ fileCountWhichHasSmaller1PE;
//            else if(peOfUserFile > 1.0)
//                ++ fileCountWhichHasBigger1PE;
//
//
//            //detect number of files which has PE smaller than 0.5
//            if(peOfUserFile < 0.5)
//                ++ fileCountWhichHasSmallerZeroPointFivePE;
//
//
//            //check which has bigger 2 or equal to PE and smaller to 2 PE
//            if(peOfUserFile < 2.0)
//                ++ fileCountWhichHasSmaller2PE;
//            else if(peOfUserFile > 2.0 || Double.compare(peOfUserFile, 2.0) == 0)
//            {
//                ++fileCountWhichHasBiggerEqualTo2PE;
//                //filePathsBiggerEqualTo2PE.add(ppUserFilePath);
//            }
//
//
//            //check how many users are not predictable
//            if(Double.compare(peOfUserFile, 1.0) == 0)
//                ++ fileCountWhichHas1PE;
//
//
//            //update the sum
//            sumOfPEsOfUsers += peOfUserFile;
//            System.out.println("PE of file with name: " + Utils.fileNameFromPath(ppUserFilePath) + " => " + peOfUserFile);
//            System.out.println("Current average PE of all files: " + sumOfPEsOfUsers / fileCount);
//            System.out.println(String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
//                    + " % of the current processed files has PE < 1.0");
//            System.out.println(String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
//                    + " % of the current processed files has PE < 0.5");

            //System.out.println("-----------------------------------------------------------------------\n");


            gaussianizedVisits.clear(); //significantUniqueLocations.clear();
            //break;
        } // for


        //now calculate average proportion of time spent by each user in significant places
        //by the division of number of users
        avgProportionOfTimeSpentByEachUserInSignificantPlaces = avgProportionOfTimeSpentByEachUserInSignificantPlaces / ppUserFilePaths.size();


//        calculate PE_global which is the predictability error of the dataset
//        double PE_global = sumOfPEsOfUsers / ppUserFilePaths.size();


        //System.out.println("|| totalResSecondsOfAllVisitsOfUsersBeforePreprocessing => " + totalResSecondsOfAllVisitsOfUsersBeforePreprocessing);
        //System.out.println("|| totalResSecondsOfAllVisitsOfUsersAfterPreprocessing => " + totalResSecondsOfAllVisitsOfUsersAfterPreprocessing);
        //System.out.println("|| Total number of locations (visits) of users before pre-processing => " + countOfAllLocationsOfUsers);
        //System.out.println("|| Total number of unique locations before pre-processing => " + globalUniqueLocations.size());


        //System.out.println("|| Number of users: " + ppUserFilePaths.size());
        //System.out.println("|| Total number of (significant) visits: " + totalNumberOfSignificantVisits);
        //System.out.println("|| Total number of unique significant locations: " + globalUniqueSignificantLocations.size());




        /*
        System.out.println("|| Average number of significant locations per user: "
                + String.format("%.2f",
                //+ globalUniqueSignificantLocations.size()
                //+ (numberOfAllUniqueLocations - allUniqueLocations.size())
                + countOfAllSignificantLocationsOfUsers
                        / (double) ppUserFilePaths.size()
                ));
        */




        //System.out.println("|| Average number of (significant) visits per user: " + (int) (totalNumberOfSignificantVisits / (double) ppUserFilePaths.size()));
        //System.out.println("|| Average residence time in a place D (seconds): "
        //        + (int) (
        //        //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing
        //        totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
        //                /// ( 1.0 * globalUniqueSignificantLocations.size() )
        //                /// ( 1.0 * countOfAllSignificantLocationsOfUsers )
        //                /// ( 1.0 * globalUniqueLocations.size())
        //                / (1.0 * countOfAllLocationsOfUsers)
        //));
        //System.out.println("|| Total trace length in days: " + dataset.traceLengthInDays());




        /*
        System.out.println("|| Average proportion of time spent by each user in significant places: "
                //+ String.format("%.2f", totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing)
                + String.format("%.2f", avgProportionOfTimeSpentByEachUserInSignificantPlaces)
                + " %");
        */




//        System.out.println("|| " + new Date() + " => PE of " + dataset + " is " + PE_global);
//        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmaller1PE * 100 / (1.0 * fileCount))
//                + " % of the dataset has PE < 1.0");
//        System.out.println("|| " + String.format("%.2f", fileCountWhichHasSmallerZeroPointFivePE * 100 / (1.0 * fileCount))
//                + " % of the dataset has PE < 0.5");
//        System.out.println("|| Number of users which has PE > 1.0: " + fileCountWhichHasBigger1PE);
//        System.out.println("|| Number of users which has PE < 2.0: " + fileCountWhichHasSmaller2PE);
//        System.out.println("|| Number of users which has PE >= 2.0: " + fileCountWhichHasBiggerEqualTo2PE);
//
//        System.out.println("|| Number of users which are not predictable (has PE == 1.0): " + fileCountWhichHas1PE);


        //System.out.println("|| Max cumulative weighted gaussian value of the dataset = " + maxCumulativeWeightedGaussianValueOfTheDataset);
        //System.out.println("|| Min cumulative weighted gaussian value of the dataset = " + minCumulativeWeightedGaussianValueOfTheDataset);


        return new Tuple2<Double, Tuple2<Double, Double>>(thresholdT, new Tuple2<Double, Double>(
                countOfAllSignificantLocationsOfUsers / (double) ppUserFilePaths.size(),
                //globalUniqueSignificantLocations.size() / (double) ppUserFilePaths.size(),

                avgProportionOfTimeSpentByEachUserInSignificantPlaces
                //totalResSecondsOfAllVisitsOfUsersAfterPreprocessing * 100.0 / totalResSecondsOfAllVisitsOfUsersBeforePreprocessing
        ));

    } // testClusteringOnCenceMeGPSDataset



    //helper method to print unmerged original visits from raw user files
    private static void printOriginalUnmergedVisitsOfAllUsers(Dataset dataset, String sortedXMLFilePath)
    {
        //get the original raw files
        List<String> userRawDataFilePaths = Preprocessing.getUserRawDataFilePaths(sortedXMLFilePath, dataset);
        //iterate and print pre-processed original unmerged visits from that raw files
        for(String userRawDataFilePath : userRawDataFilePaths)
        {
            Preprocessing.printSortedOriginalVisitsFromRawFile(userRawDataFilePath, dataset);
            //break; // print for the first file only
        } // for

    } // printOriginalUnmergedVisitsOfAllUsers


    //helper method to generate signficint location processing results
    private void gpsSignificantLocationExtractionResults(JavaSparkContext sc)
    {
        ///*
        Dataset dataset
                //= Dataset.Cabspotting;
                = Dataset.CenceMeGPS;
        String localDirPathString
                //= "raw/Cabspotting/data_all_loc/";
                = "raw/CenceMeGPS/data_all_loc/";



        String[] deserializationPaths
                = new String[]{localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap.ser"};
//              = new String[]
//                {
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 1 +  ".ser",
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 2 +  ".ser",
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 3 +  ".ser",
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 4 +  ".ser",
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 5 +  ".ser",
//                        localDirPathString + "serialization/userFileNameLocationWeightedCumulativeGaussianMap"
//                                + 6 +  ".ser",
//                };


        //DBScan parameters
        double epsilon = 100; //200; // in meters
        int minPoints = 18; //18; // start clustering if there are at least minPoints points in proximity

        ///*
        //0.0016 from   0.0000000001 with steps of 0.0000000001 = 15,999,999 iterations => Cabspotting
        //0.00075 from  0.000000068  with steps of 0.000000001 = 749,932 iterations => CenceMeGPS
        double thresholdStep
                        = 5.0E-7; //= 5.0E-6; // = 1.0E-9;  // CenceMeGPS
                        //= 5.0E-9; //= 5.0E-10;   //= 1.0E-10; // Cabspotting
                        //= 0.00001;
        double initialThreshold
                        = thresholdStep; //= 0.000000068;  // CenceMeGPS
                        //= 0.0000000001;   // Cabspotting
                        //= 0.00001;
        double endThreshold
                        = 0.00075; //= 4.0E-6; // = 0.00075;      // CenceMeGPS
                        //= 3.0E-7; //= 1.0E-7; = 0.0016;         // Cabspotting
                        //= 0.00025;


        ArrayList<Double> thresholds = new ArrayList<>();
        for(double thresholdT = initialThreshold;
            thresholdT < endThreshold || Double.compare(thresholdT, endThreshold) == 0;
            thresholdT += thresholdStep)
        {
            thresholds.add(thresholdT);
        } // for
        System.out.println(thresholds.size());




        Map<Double, Triple<Double, Double, Double>> resultsMap =
                Utils.distributedGPSAvgSignLocAvgSignTime(sc, dataset,
                    localDirPathString, false, thresholds,
                    epsilon, minPoints); //, deserializationPaths);

                //Utils.distributedGPSAvgSignLocAvgSignTime(sc, dataset, localDirPathString, false,
                //        epsilon, minPoints, thresholds); //, deserializationPaths);


                //Utils.distributedGPSAvgSignLocAvgSignTime(sc, dataset,
                //        Utils.sample(Utils.listFilesFromLocalPath(localDirPathString, false),5),
                //        epsilon, minPoints, thresholds);


                //Utils.distributedGPSAvgSignLocAvgSignTimeInChunks(sc, dataset,
                //        localDirPathString, false, thresholds,
                //        epsilon, minPoints, deserializationPaths);

                //Utils.deserializeAndCollect(Utils.listFilesFromLocalPath("raw/Cabspotting/data_all_loc/serialization3",
                //        false), 536);

//        TreeMap<Double, Tuple2<Double, Double>> origResultsMap = new TreeMap<>(resultsMap);
//        resultsMap = new LinkedHashMap<>();
//        for(Double threshold : thresholds)
//        {
//            Map.Entry<Double, Tuple2<Double, Double>> low = origResultsMap.floorEntry(threshold);
//            Map.Entry<Double, Tuple2<Double, Double>> high = origResultsMap.ceilingEntry(threshold);
//            Tuple2<Double, Double> res = null;
//            if (low != null && high != null) {
//                res = Math.abs(threshold-low.getKey()) < Math.abs(threshold-high.getKey())
//                        ?   low.getValue()
//                        :   high.getValue();
//            } else if (low != null || high != null) {
//                res = low != null ? low.getValue() : high.getValue();
//            }
//
//            resultsMap.put(threshold, res);
//        } // for


//        StringBuilder thresholdAvgSignLocStringBuilder = new StringBuilder("");
//        StringBuilder thresholdAvgSignTimeStringBuilder = new StringBuilder("");
//        StringBuilder thresholdStringBuilder = new StringBuilder("");
//        StringBuilder avgSignLocStringBuilder = new StringBuilder("");
//        StringBuilder avgSignTimeStringBuilder = new StringBuilder("");
//        for(Double thresholdT : resultsMap.keySet())
//        {
//            Tuple2<Double, Double> tuple2 = resultsMap.get(thresholdT);
//            double avgSignLoc = tuple2._1;
//            double avgSignTime = tuple2._2;
//            thresholdAvgSignLocStringBuilder.append(thresholdT).append(",").append(avgSignLoc).append("\n");
//            thresholdAvgSignTimeStringBuilder.append(thresholdT).append(",").append(avgSignTime).append("\n");
//
//            thresholdStringBuilder.append(thresholdT).append(",").append("\n");
//            avgSignLocStringBuilder.append(avgSignLoc).append(",").append("\n");
//            avgSignTimeStringBuilder.append(avgSignTime).append(",").append("\n");
//        } // for
//        String appendix = "[]_eps=" + epsilon + "_minPts=" + minPoints
//                + "_from_" + initialThreshold + "_to_" + endThreshold + "_in_steps_of_" + thresholdStep + "";
//        Utils.writeToLocalFileSystem(localDirPathString + "plot/estimates/_.csv",
//                "_thresholdT" + appendix + "_",
//                "", ".csv", thresholdStringBuilder.toString());
//        Utils.writeToLocalFileSystem(localDirPathString + "plot/estimates/_.csv",
//                "_thresholdTAvgSignLoc" + appendix + "_",
//                "", ".csv", thresholdAvgSignLocStringBuilder.toString());
//        Utils.writeToLocalFileSystem(localDirPathString + "plot/estimates/_.csv",
//                "_thresholdTAvgSignTime" + appendix + "_",
//                "", ".csv", thresholdAvgSignTimeStringBuilder.toString());
//        Utils.writeToLocalFileSystem(localDirPathString + "plot/estimates/_.csv",
//                "_avgSignLoc" + appendix + "_",
//                "", ".csv", avgSignLocStringBuilder.toString());
//        Utils.writeToLocalFileSystem(localDirPathString + "plot/estimates/_.csv",
//                "_avgSignTime" + appendix + "_",
//                "", ".csv", avgSignTimeStringBuilder.toString());


        //---- build python file ----
        String appendix = "_eps=" + epsilon + "_minPts=" + minPoints
                + "_from_" + initialThreshold + "_to_" + endThreshold + "_in_steps_of_" + thresholdStep + "_";

        //avg significant locations
        StringBuilder pfsb1 = new StringBuilder();
        pfsb1.append("import matplotlib.pyplot as plt").append("\n");
        pfsb1.append("X = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb1.append(threshold).append(",").append("\n");
        } // for
        pfsb1.append("]").append("\n");
        pfsb1.append("\n");
        pfsb1.append("Y = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb1.append(resultsMap.get(threshold).getLeft()).append(",").append("\n");
        } // for
        pfsb1.append("]").append("\n");
        pfsb1.append("\n");
        pfsb1.append("plt.plot(X, Y, 'r-o')").append("\n");
        pfsb1.append("plt.ylabel('Average number significant locations per user')").append("\n");
        pfsb1.append("plt.xlabel('Threshold')").append("\n");
        pfsb1.append("plt.show()").append("\n");

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_avg_sign_loc_per_user_" + appendix,
                    ".py", pfsb1.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_avg_sign_loc_per_user_" + appendix,
                    ".py", pfsb1.toString());


        //avg significant time
        StringBuilder pfsb2 = new StringBuilder();
        pfsb2.append("import matplotlib.pyplot as plt").append("\n");
        pfsb2.append("X = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb2.append(threshold).append(",").append("\n");
        } // for
        pfsb2.append("]").append("\n");
        pfsb2.append("\n");
        pfsb2.append("Y = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb2.append(resultsMap.get(threshold).getMiddle()).append(",").append("\n");
        } // for
        pfsb2.append("]").append("\n");
        pfsb2.append("\n");
        pfsb2.append("plt.plot(X, Y, 'r-o')").append("\n");
        pfsb2.append("plt.ylabel('Significant time per user [%]')").append("\n");
        pfsb2.append("plt.xlabel('Threshold')").append("\n");
        pfsb2.append("plt.show()").append("\n");

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_avg_sign_time_per_user_" + appendix,
                    ".py", pfsb2.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_avg_sign_time_per_user_" + appendix,
                    ".py", pfsb2.toString());

        //total outlier (above threshold) gps points
        StringBuilder pfsb3 = new StringBuilder();
        pfsb3.append("import matplotlib.pyplot as plt").append("\n");
        pfsb3.append("X = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb3.append(threshold).append(",").append("\n");
        } // for
        pfsb3.append("]").append("\n");
        pfsb3.append("\n");
        pfsb3.append("Y = [").append("\n");
        for (double threshold : resultsMap.keySet()) {
            pfsb3.append(resultsMap.get(threshold).getRight()).append(",").append("\n");
        } // for
        pfsb3.append("]").append("\n");
        pfsb3.append("\n");
        pfsb3.append("plt.plot(X, Y, 'r-o')").append("\n");
        pfsb3.append("plt.ylabel('Total outlier above threshold GPS points')").append("\n");
        pfsb3.append("plt.xlabel('Threshold')").append("\n");
        pfsb3.append("plt.show()").append("\n");

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_total_outlier_above_threshold_gps_points_" + appendix,
                    ".py", pfsb3.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_python_",
                    "_threshold_vs_total_outlier_above_threshold_gps_points_" + appendix,
                    ".py", pfsb3.toString());

        //---- build python file ----


        //---- build structure for latex plot ----
        StringBuilder latexSb1 = new StringBuilder("");
        for (double threshold : resultsMap.keySet())
        {
            latexSb1.append("(").append(threshold).append(",").append(resultsMap.get(threshold).getLeft()).append(")").append("\n");
        } // for


        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_avg_sign_loc_per_user_" + appendix,
                    ".txt", latexSb1.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_avg_sign_loc_per_user_" + appendix,
                    ".txt", latexSb1.toString());


        StringBuilder latexSb2 = new StringBuilder("");
        for (double threshold : resultsMap.keySet())
        {
            latexSb2.append("(").append(threshold).append(",").append(resultsMap.get(threshold).getMiddle()).append(")").append("\n");
        } // for

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_avg_sign_time_per_user_" + appendix,
                    ".txt", latexSb2.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_avg_sign_time_per_user_" + appendix,
                    ".txt", latexSb2.toString());

        StringBuilder latexSb3 = new StringBuilder("");
        for (double threshold : resultsMap.keySet())
        {
            latexSb3.append("(").append(threshold).append(",").append(resultsMap.get(threshold).getRight()).append(")").append("\n");
        } // for

        if (dataset == Dataset.CenceMeGPS)
            Utils.writeToLocalFileSystem("raw/CenceMeGPS/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_total_outlier_above_threshold_gps_points_" + appendix,
                    ".txt", latexSb3.toString());
        else if (dataset == Dataset.Cabspotting)
            Utils.writeToLocalFileSystem("raw/Cabspotting/data_all_loc/plot/_.csv", "_latex_",
                    "_threshold_vs_total_outlier_above_threshold_gps_points_" + appendix,
                    ".txt", latexSb3.toString());
        //---- build structure for latex plot ----

        Utils.serialize(localDirPathString + "plot/resultsMap" + "_eps=" + epsilon + "_minPts=" + minPoints
                + "_from_" + initialThreshold + "_to_" + endThreshold + "_in_steps_of_" + thresholdStep + "_" +".ser", resultsMap);

        //*/
    } // gpsSignificantLocationExtractionResults

} // class Launcher















/*
public class ScatterDemo extends AbstractAnalysis
    {
        public static void main(String[] args) throws Exception
        {
            ScatterDemo demo = new ScatterDemo();
            AnalysisLauncher.open(demo);
        }
        @Override
        public void init()
        {
            String content = Utils.fileContentsFromLocalFilePath("raw/CenceMeGPS/data_all_loc/plot/_plot_/__plot_.csv");
            String[] lines = content.split("\\n");



            int size = lines.length; //500000;
            double x;
            double y;
            double z;
            double a;

            Coord3d[] points = new Coord3d[size];
            Color[]   colors = new Color[size];

            //Random r = new Random();
            //r.setSeed(0);

            //for(int i=0; i<size; i++)
            for(int i = 0; i < lines.length; i ++)
            {
                println(lines[i]);
                String[] lineComps = lines[i].split(",");

                x = Double.parseDouble(lineComps[0]); //r.nextFloat() - 0.5f;
                y = Double.parseDouble(lineComps[1]); //r.nextFloat() - 0.5f;
                z = Double.parseDouble(lineComps[2]); //r.nextFloat() - 0.5f;

                points[i] = new Coord3d(x, y, z);
                a = 0.25f;
                colors[i] = new Color((float) x, (float) y, (float) z, (float) a);
            }

            Scatter scatter = new Scatter(points, colors);
            chart = AWTChartComponentFactory.chart(Quality.Advanced, "newt");
            chart.getScene().add(scatter);
        }
    } // class ScatterDemo
*/