package com.rb.nextplace.distributed;


import com.google.common.collect.HashMultiset;
import com.sun.org.apache.xerces.internal.dom.DocumentImpl;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.spark.api.java.JavaSparkContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import scala.Tuple2;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.util.*;

//class to contain pre-processing related methods
public class Preprocessing
{
    //helper method to generate pre-processed merged visits of users
    //instead of generating pre-processed user file paths as in the method of generatePPUserFilePaths()
    //it will return listTuple2 with userName key and ArrayList<Visit> value
    public static List<Tuple2<String, ArrayList<Visit>>> generatePPMergedVisitsOfUsers(JavaSparkContext sc,
                                                                                       String pathOfXmlFileContainingUserFileNames,
                                                                                       Dataset dataset, int deltaThresholdInSecondsToMerge,
                                                                                       double significantLocationThreshold,
                                                                                       double sigma,
                                                                                       Double eps, Integer minPts)
    {
        if(dataset != null)
        {
            System.out.println("Pre-processing " + dataset + " .....");
            dataset.printInfo();
        } // if

        //pre-processed merged visits of users
        //Tuple2 can be converted to JavaPairRDD easily
        List<Tuple2<String, ArrayList<Visit>>> listTuple2OfPPUserFilePathAndMergedVisits
                = new ArrayList<Tuple2<String, ArrayList<Visit>>>();

        //read the xml file and get file names from it and pre-process them
        try
        {
            //e.g. sorted _cabs.xml file should have the structure of the following:
            //<cabs>
            //      <cab id="abboip" updates="22717"/>
            //       ...
            //</cabs>
            File sortedXmlFile = new File(pathOfXmlFileContainingUserFileNames);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(sortedXmlFile);

            //NodeList nodeList = doc.getElementsByTagName("cab");
            NodeList nodeList = doc.getElementsByTagName(dataset.userTagName());
            List<Node> nodes = new ArrayList<Node>();
            for (int index = 0; index < nodeList.getLength(); index++) {
                nodes.add(nodeList.item(index));
            } // for



            //for counting number of users whether they are found or not
            int userNo = 0;


            //array list to hold found file names;
            //because findFileName() can have multiple results
            //this array list will ensure that the same file is not processed twice or more
            List<String> foundFileNames = new ArrayList<String>();


            //traverse through nodes, extract file names
            for(Node node: nodes)
            {
                //String userName = ((Element) node).getAttribute("id");
                String userName = ((Element) node).getAttribute(dataset.userAttributeName()); //+ ".txt"; => when .equals() is used findFileByName()
                File foundFile = findFileByName(new File(sortedXmlFile.getParent() /*"resources"*/),
                        userName);  // real file names are actually derived from user names

                //increment userNo, since for loop loops through all users one by one
                //check whether they are found or not
                //userNo is incremented regardless of this case
                userNo ++;


                //if the file is found
                if(foundFile != null)
                {
                    //--------------------------------------------
                    // the file is found; we can do pre-processing
                    //--------------------------------------------

                    //get the file name (which also includes its extension)
                    String foundFileName = foundFile.getName();

                    //check whether we have processed the file already; if we have, then continue the loop
                    if(foundFileNames.contains(foundFileName)) continue;
                    else
                    {
                        //the file is new, therefore do-preprocessing
                        //update the array list as well
                        foundFileNames.add(foundFileName);
                    } // else


                    //at this point we have a new file to pre-processes;
                    //so get the extension of a file;
                    //getExtension() returns txt, xml and so on;
                    //therefore, prepend it with .
                    String fileExtension = "." + FilenameUtils.getExtension(foundFileName);


                    //generate user info first
                    System.out.println("======= User " + userNo
                            + ": File found by matching " + dataset.userTagName() + " "
                            + dataset.userAttributeName() + " element with file name: "
                            + foundFileName + " =======");

                    System.out.println("======= Pre-processing file with name: "
                            + foundFileName + " ..... =======\n");


                    //we defined out of try block to close them in finally block
                    PrintWriter ppUserFileWriter = null;
                    PrintWriter ppResultsFileWriter = null;

                    try
                    {
                        //------------------------------------------------------------------------------------------
                        //file path for pre-processed file that will be created
                        String ppUserFilePath = foundFile.getPath()
                                .replace(fileExtension, "")
                                + "_preprocessed" + fileExtension;
                        ///*
                        //resulting user file which is sorted and pre-processed for the algorithm
                        File ppUserFile = new File(ppUserFilePath);

                        //create the file
                        ppUserFile.createNewFile();

                        //print writer for resultUserFile
                        //will write merged visits to the result user file
                        ppUserFileWriter = new PrintWriter(ppUserFile);
                        //*/
                        //------------------------------------------------------------------------------------------


                        //------------------------------------------------------------------------------------------
                        //now create file and print writer to write the results of pre-processing to the file
                        //this file will have more information as compared to *_preprocessed.txt file
                        File ppResultsFile = new File(foundFile.getPath()
                                .replace(fileExtension, "")
                                + "_Preprocessing_Results" + fileExtension);

                        //create that file
                        ppResultsFile.createNewFile();

                        //now create print writer from that file
                        ppResultsFileWriter = new PrintWriter(ppResultsFile);
                        //------------------------------------------------------------------------------------------



                        //we created a method, that works for each dataset defined in enum;
                        //different pre-processing technique will be applied
                        //based on dataset parameter given to the call;
                        //generateSortedOriginalVisitsFromRawFile() will return sorted
                        //visits in chronological order pre-processed from the raw file
                        TreeSet<Visit> sortedVisits
                                = generateSortedOriginalVisitsFromRawFile(userName, foundFile.getPath(), dataset);
                        //convert to array list for mergeVisits() method
                        //will take O(n), as compared to Collections.sort() which can take O(nlogn) time.
                        ArrayList<Visit> visits = new ArrayList<Visit>(sortedVisits);



                        //------------------------------------------------------------------------------------------
                        /*
                        System.out.println("================ Original unmerged visits of the user: "
                                                                                    + userName + " ================");
                        for(int index = 0; index < visits.size(); index ++) {
                            System.out.println("#" + (index + 1) + ": " + visits.get(index).toStringWithDate());
                            //if(index >= 8000) break;
                        }
                        */

                        /*
                        preProcessingResultsFileWriter.println("================ Original unmerged visits of the user: "
                                                                + userName + " ================");
                        for(int index = 0; index < visits.size(); index ++)
                        {
                            preProcessingResultsFileWriter.println("*" + (index + 1) + ": "
                                                                + visits.get(index).toStringWithDate());
                        } // for
                        */
                        //------------------------------------------------------------------------------------------



                        //generate data set by merging visits
                        //merge operation will create visits with non-zero residence seconds
                        //you can also call
                        //=== mergeVisitsWithDateCalculation(visits, deltaThresholdInSeconds); ==
                        //to get the same result;
                        //provided visits should be in chronological order;
                        //since every visit's at t_i can be merged visit at t_i+1 or further
                        //returned mergedVisits are in chronological order
                        ArrayList<Visit> mergedVisits = mergeVisits(dataset, visits, deltaThresholdInSecondsToMerge);//, customComparator);

                        //Apply 2D Gaussian distribution for GPS datasets (0.10 for Cabspotting and 0.15 for CenceMeGPS)
                        //and visit frequency threshold for WiFi datasets (20 for DartmouthWiFi and IleSansFils)
                        mergedVisits = getVisitsToSignificantLocations(sc, dataset, ppUserFile.getName(), mergedVisits, significantLocationThreshold, sigma,
                                eps, minPts);


                        //------------------------------------------------------------------------------------------
                        /*
                        for(int index = 0; index < mergedVisits.size(); index ++)
                        {
                            System.out.println("#" + (index + 1) + ": " + mergedVisits.get(index).toStringWithDate());
                        } // for
                        */

                        //write to the pre-processing results file
                        ppResultsFileWriter.println("================ Merged visits of the user: "
                                + userName + " ================");
                        for(int index = 0; index < mergedVisits.size(); index ++)
                            ppResultsFileWriter.println("#" + (index + 1) + ": "
                                    + mergedVisits.get(index).toStringWithDate());
                        //------------------------------------------------------------------------------------------


                        //------------------------------------------------------------------------------------------
                        ///*
                        //now populate pre-processFile with structured data
                        for(Visit v : mergedVisits)
                        {
                            String user = v.getUserName();
                            String locationName = v.getLocation().getName();
                            double latitude = v.getLocation().getLatitude();
                            double longitude = v.getLocation().getLongitude();
                            //getTime() returns: the number of milliseconds since January 1, 1970,
                            //00:00:00 GMT represented by the given date;
                            long unixTimeStampInMilliseconds = v.getTimeSeriesInstant().getArrivalTime().getTime();
                            long residenceTimeInSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                            //one line will be comma-separated list of values;
                            //therefore, they will be easily splittable by String.split() method later
                            ppUserFileWriter.println(user + "," + locationName + "," + latitude + "," + longitude + ","
                                    + unixTimeStampInMilliseconds + "," + residenceTimeInSeconds);
                        } // for
                        //*/
                        //------------------------------------------------------------------------------------------


                        //at the end populate TreeMap with user and merged visits pairs to be returned from the method
                        Tuple2<String, ArrayList<Visit>> tuple2StringArrayList
                                = new Tuple2<String, ArrayList<Visit>>(ppUserFilePath, mergedVisits);
                        listTuple2OfPPUserFilePathAndMergedVisits.add(tuple2StringArrayList);
                    } // try
                    catch(IOException iox)
                    {
                        iox.printStackTrace();
                    } // catch
                    finally
                    {
                        //close print writers
                        if(ppUserFileWriter != null)
                            ppUserFileWriter.close();

                        if(ppResultsFileWriter != null)
                            ppResultsFileWriter.close();
                    } // finally
                } // if
                else
                {
                    //Do nothing for the files those are not found
                    //System.out.println(userNo + ": File not found by matching cab id element with file name");
                } // else


                //break to process one file only
                //break;
            } // for

            //reset userNo
            userNo = 0;

        } // try
        catch(ParserConfigurationException pcex)
        {
            pcex.printStackTrace();
        } // catch
        catch(SAXException saxex)
        {
            saxex.printStackTrace();;
        } // catch
        catch(IOException ioex)
        {
            ioex.printStackTrace();
        } // catch


        return listTuple2OfPPUserFilePathAndMergedVisits;
    } // generatePPMergedVisitsOfUsers

    //method to get the pre-processed file paths
    public static ArrayList<String> generatePPUserFilePaths(JavaSparkContext sc,
                                                            String pathOfXmlFileContainingUserFileNames,
                                                            Dataset dataset,
                                                            int deltaThresholdInSecondsToMerge,
                                                            double significantLocationThreshold,
                                                            double sigma,
                                                            Double eps, Integer minPts) // DBSCAN parameters
    {
        if(dataset != null)
        {
            System.out.println("Pre-processing " + dataset + " .....");
            dataset.printInfo();
        } // if

        //pre-processed user file paths
        ArrayList<String> ppFilePaths = new ArrayList<String>();

        //read the xml file and get file names from it and pre-process them
        try
        {
            //e.g. sorted _cabs.xml file should have the structure of the following:
            //<cabs>
            //      <cab id="abboip" updates="22717"/>
            //       ...
            //</cabs>
            File sortedXmlFile = new File(pathOfXmlFileContainingUserFileNames);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(sortedXmlFile);

            //NodeList nodeList = doc.getElementsByTagName("cab");
            NodeList nodeList = doc.getElementsByTagName(dataset.userTagName());
            List<Node> nodes = new ArrayList<Node>();
            for (int index = 0; index < nodeList.getLength(); index++) {
                nodes.add(nodeList.item(index));
            } // for



            //for counting number of users whether they are found or not
            int userNo = 0;


            //array list to hold found file names;
            //because findFileName() can have multiple results
            //this array list will ensure that the same file is not processed twice or more
            List<String> foundFileNames = new ArrayList<String>();


            //traverse through nodes, extract file names
            for(Node node: nodes)
            {
                //String userName = ((Element) node).getAttribute("id");
                String userName = ((Element) node).getAttribute(dataset.userAttributeName()); //+ ".txt"; => when .equals() is used findFileByName()
                File foundFile = findFileByName(new File(sortedXmlFile.getParent() /*"resources"*/),
                        userName);  // real file names are actually derived from user names

                //if(!userName.equals("CenceMeLiteLog19")) continue;

                //increment userNo, since for loop loops through all users one by one
                //check whether they are found or not
                //userNo is incremented regardless of this case
                userNo ++;


                //if the file is found
                if(foundFile != null)
                {
                    //--------------------------------------------
                    // the file is found; we can do pre-processing
                    //--------------------------------------------

                    //get the file name (which also includes its extension)
                    String foundFileName = foundFile.getName();

                    //check whether we have processed the file already; if we have, then continue the loop
                    if(foundFileNames.contains(foundFileName)) continue;
                    else
                    {
                        //the file is new, therefore do-preprocessing
                        //update the array list as well
                        foundFileNames.add(foundFileName);
                    } // else


                    //at this point we have a new file to pre-processes;
                    //so get the extension of a file;
                    //getExtension() returns txt, xml and so on;
                    //therefore, prepend it with .
                    String fileExtension = "." + FilenameUtils.getExtension(foundFileName);


                    //generate user info first
                    System.out.println("======= User " + userNo
                            + ": File found by matching " + dataset.userTagName() + " "
                            + dataset.userAttributeName() + " element with file name: "
                            + foundFileName + " =======");

                    System.out.println("======= Pre-processing file with name: "
                            + foundFileName + " ..... =======\n");


                    //we defined out of try block to close them in finally block
                    PrintWriter ppUserFileWriter = null;
                    PrintWriter ppResultsFileWriter = null;

                    try
                    {
                        //------------------------------------------------------------------------------------------
                        //file path for pre-processed file that will be created
                        String ppUserFilePath = foundFile.getPath()
                                .replace(fileExtension, "")
                                + "_preprocessed" + fileExtension;
                        ///*
                        //resulting user file which is sorted and pre-processed for the algorithm
                        File ppUserFile = new File(ppUserFilePath);


                        //result user file will have the following structure
                        //"latitude,longitude,unixTimeStamp,residenceTimeInSeconds" => which is actually a merged visit
                        //unixTimeStamp can be converted to Date and arrivalTimeInDaySeconds
                        //after reading the file somewhere else;
                        //now add the result user file path to "preProcessedFilePaths"
                        ppFilePaths.add(ppUserFile.getPath());

                        //print writer for resultUserFile
                        //will write merged visits to the result user file
                        ppUserFileWriter = new PrintWriter(ppUserFile);
                        //------------------------------------------------------------------------------------------


                        //------------------------------------------------------------------------------------------
                        //now create file and print writer to write the results of pre-processing to the file
                        //this file will have more information as compared to *_preprocessed.txt file
                        File ppResultsFile = new File(foundFile.getPath()
                                .replace(fileExtension, "")
                                + "_Preprocessing_Results" + fileExtension);

                        //create that file
                        ppResultsFile.createNewFile();

                        //now create print writer from that file
                        ppResultsFileWriter = new PrintWriter(ppResultsFile);
                        //------------------------------------------------------------------------------------------



                        //we created a method, that works for each dataset defined in enum;
                        //different pre-processing technique will be applied
                        //based on dataset parameter given to the call;
                        //generateSortedOriginalVisitsFromRawFile() will return sorted
                        //visits in chronological order pre-processed from the raw file
                        TreeSet<Visit> sortedVisits
                                = generateSortedOriginalVisitsFromRawFile(userName, foundFile.getPath(), dataset);
                        //convert to array list for mergeVisits() method
                        //will take O(n), as compared to Collections.sort() which can take O(nlogn) time.
                        ArrayList<Visit> visits = new ArrayList<Visit>(sortedVisits);



                        //------------------------------------------------------------------------------------------
                        /*
                        System.out.println("================ Original unmerged visits of the user: "
                                                                                    + userName + " ================");
                        for(int index = 0; index < visits.size(); index ++) {
                            System.out.println("#" + (index + 1) + ": " + visits.get(index).toStringWithDate());
                            //if(index >= 8000) break;
                        }
                        */

                        /*
                        preProcessingResultsFileWriter.println("================ Original unmerged visits of the user: "
                                                                + userName + " ================");
                        for(int index = 0; index < visits.size(); index ++)
                        {
                            preProcessingResultsFileWriter.println("*" + (index + 1) + ": "
                                                                + visits.get(index).toStringWithDate());
                        } // for
                        */
                        //------------------------------------------------------------------------------------------


                        //generate data set by merging visits
                        //merge operation will create visits with non-zero residence seconds
                        //you can also call
                        //=== mergeVisitsWithDateCalculation(visits, deltaThresholdInSeconds); ==
                        //to get the same result;
                        //provided visits should be in chronological order;
                        //since every visit's at t_i can be merged visit at t_i+1 or further
                        //returned mergedVisits are in chronological order
                        ArrayList<Visit> mergedVisits = mergeVisits(dataset, visits, deltaThresholdInSecondsToMerge);

                        //SHORTCUT WAY OF OBTAINING MERGED VISITS
                        //ArrayList<Visit> mergedVisits = Utils.correspondingMergedVisits(dataset, ppUserFile.getName(), false);

                        //Apply 2D Gaussian distribution for GPS datasets (0.10 for Cabspotting and 0.15 for CenceMeGPS)
                        //and visit frequency threshold for WiFi datasets (20 for DartmouthWiFi and IleSansFils)
                        mergedVisits = getVisitsToSignificantLocations(sc, dataset, ppUserFile.getName(),
                                        mergedVisits, significantLocationThreshold, sigma,
                                        eps, minPts);


                        //------------------------------------------------------------------------------------------
                        /*
                        for(int index = 0; index < mergedVisits.size(); index ++)
                        {
                            System.out.println("#" + (index + 1) + ": " + mergedVisits.get(index).toStringWithDate());
                        } // for
                        */

                        //write to the pre-processing results file
                        ppResultsFileWriter.println("================ Merged visits of the user: "
                                + userName + " ================");
                        for(int index = 0; index < mergedVisits.size(); index ++)
                            ppResultsFileWriter.println("#" + (index + 1) + ": "
                                    + mergedVisits.get(index).toStringWithDate());
                        //------------------------------------------------------------------------------------------


                        //------------------------------------------------------------------------------------------
                        //now populate pre-processed file with structured data
                        for(Visit v : mergedVisits)
                        {
                            String user = v.getUserName();
                            String locationName = v.getLocation().getName();
                            double latitude = v.getLocation().getLatitude();
                            double longitude = v.getLocation().getLongitude();
                            //getTime() returns: the number of milliseconds since January 1, 1970,
                            //00:00:00 GMT represented by the given date;
                            long unixTimeStampInMilliseconds = v.getTimeSeriesInstant().getArrivalTime().getTime();
                            long residenceTimeInSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                            //one line will be comma-separated list of values;
                            //therefore, they will be easily splittable by String.split() method later
                            ppUserFileWriter.println(user + "," + locationName + "," + latitude + "," + longitude + ","
                                    + unixTimeStampInMilliseconds + "," + residenceTimeInSeconds);
                        } // for
                        //------------------------------------------------------------------------------------------

                    } // try
                    catch(IOException iox)
                    {
                        iox.printStackTrace();
                    } // catch
                    finally
                    {
                        //close print writers
                        if(ppUserFileWriter != null)
                            ppUserFileWriter.close();

                        if(ppResultsFileWriter != null)
                            ppResultsFileWriter.close();
                    } // finally
                } // if
                else
                {
                    //Do nothing for the files those are not found
                    //System.out.println(userNo + ": File not found by matching cab id element with file name");
                } // else


                //break to process one file only
                //break;
            } // for

            //reset userNo
            userNo = 0;

        } // try
        catch(ParserConfigurationException pcex)
        {
            pcex.printStackTrace();
        } // catch
        catch(SAXException saxex)
        {
            saxex.printStackTrace();;
        } // catch
        catch(IOException ioex)
        {
            ioex.printStackTrace();
        } // catch


        return ppFilePaths;
    } // generatePPUserFilePaths


    //helper overloaded method to generate pre-processed user files from the given raw files directory instead of xml file
    //method to get the pre-processed file paths
    public static ArrayList<String> generatePPUserFilePaths(JavaSparkContext sc,
                                                            String rawFilesDirectoryPath,
                                                            Dataset dataset,
                                                            int deltaThresholdInSecondsToMerge,
                                                            double significantLocationThreshold,
                                                            double sigma,
                                                            boolean processLocalRawFilesRecursively,
                                                            Double eps, Integer minPts) // DBSCAN parameters
    {
        if (dataset != null) {
            System.out.println("Pre-processing " + dataset + " .....");
            dataset.printInfo();
        } // if

        //pre-processed user file paths
        ArrayList<String> ppFilePaths = new ArrayList<String>();

        //list files from the local file system
        List<String> rawFilePaths = Utils.listFilesFromLocalPath(rawFilesDirectoryPath, processLocalRawFilesRecursively);

        //then for each file, generate a pp user file path
        for(String rawFilePath : rawFilePaths)
        {
            //--------------------------------------------
            // now we can do pre-processing
            //--------------------------------------------
            File rawFile = new File(rawFilePath);

            //get the file name (which also includes its extension)
            String rawFileName = rawFile.getName();

            //at this point we have a new file to pre-processes;
            //so get the extension of a file;
            //getExtension() returns txt, xml and so on;
            //therefore, prepend it with .
            String fileExtension = "." + FilenameUtils.getExtension(rawFileName);

            //obtain userName by removing file extension
            String userName = rawFileName.replace(fileExtension, "");

            //print user info
            System.out.println("======= Pre-processing file with name: "
                    + rawFileName + " ..... =======\n");


            //we defined out of try block to close them in finally block
            PrintWriter ppUserFileWriter = null;
            PrintWriter ppResultsFileWriter = null;

            try
            {
                //------------------------------------------------------------------------------------------
                //file path for pre-processed file that will be created
                String ppUserFilePath = rawFile.getPath()
                        .replace(fileExtension, "")
                        + "_preprocessed" + fileExtension;
                ///*
                //resulting user file which is sorted and pre-processed for the algorithm
                File ppUserFile = new File(ppUserFilePath);


                //result user file will have the following structure
                //"latitude,longitude,unixTimeStamp,residenceTimeInSeconds" => which is actually a merged visit
                //unixTimeStamp can be converted to Date and arrivalTimeInDaySeconds
                //after reading the file somewhere else;
                //now add the result user file path to "preProcessedFilePaths"
                ppFilePaths.add(ppUserFile.getPath());

                //print writer for resultUserFile
                //will write merged visits to the result user file
                ppUserFileWriter = new PrintWriter(ppUserFile);
                //------------------------------------------------------------------------------------------


                //------------------------------------------------------------------------------------------
                //now create file and print writer to write the results of pre-processing to the file
                //this file will have more information as compared to *_preprocessed.txt file
                File ppResultsFile = new File(rawFile.getPath()
                        .replace(fileExtension, "")
                        + "_Preprocessing_Results" + fileExtension);

                //create that file
                ppResultsFile.createNewFile();

                //now create print writer from that file
                ppResultsFileWriter = new PrintWriter(ppResultsFile);
                //------------------------------------------------------------------------------------------



                //we created a method, that works for each dataset defined in enum;
                //different pre-processing technique will be applied
                //based on dataset parameter given to the call;
                //generateSortedOriginalVisitsFromRawFile() will return sorted
                //visits in chronological order pre-processed from the raw file
                TreeSet<Visit> sortedVisits
                        = generateSortedOriginalVisitsFromRawFile(userName, rawFile.getPath(), dataset);
                //convert to array list for mergeVisits() method
                //will take O(n), as compared to Collections.sort() which can take O(nlogn) time.
                ArrayList<Visit> visits = new ArrayList<Visit>(sortedVisits);

                //there is a possibility that we can get 0-sized visits, this is true especially for DartmouthWiFi dataset
                //if this is the case, then we will continue the for loop, to process the next file
                if(visits.isEmpty()) continue;


                //------------------------------------------------------------------------------------------
                /*
                System.out.println("================ Original unmerged visits of the user: "
                        + userName + " ================");
                for (int index = 0; index < visits.size(); index++)
                {
                    System.out.println("#" + (index + 1) + ": " + visits.get(index).toStringWithDate());
                    //if(index >= 8000) break;
                }
                */

                /*
                preProcessingResultsFileWriter.println("================ Original unmerged visits of the user: "
                        + userName + " ================");
                for (int index = 0; index < visits.size(); index++) {
                    preProcessingResultsFileWriter.println("*" + (index + 1) + ": "
                            + visits.get(index).toStringWithDate());
                } // for
                */
                //------------------------------------------------------------------------------------------


                //generate data set by merging visits
                //merge operation will create visits with non-zero residence seconds
                //you can also call
                //=== mergeVisitsWithDateCalculation(visits, deltaThresholdInSeconds); ==
                //to get the same result;
                //provided visits should be in chronological order;
                //since every visit's at t_i can be merged visit at t_i+1 or further
                //returned mergedVisits are in chronological order
                ArrayList<Visit> mergedVisits = mergeVisits(dataset, visits, deltaThresholdInSecondsToMerge);//, customComparator);


                //Apply 2D Gaussian distribution for GPS datasets (0.10 for Cabspotting and 0.15 for CenceMeGPS)
                //and visit frequency threshold for WiFi datasets (20 for DartmouthWiFi and IleSansFils)
                mergedVisits = getVisitsToSignificantLocations(sc, dataset, ppUserFile.getName(), mergedVisits, significantLocationThreshold, sigma,
                        eps, minPts);


                //------------------------------------------------------------------------------------------
                /*
                for (int index = 0; index < mergedVisits.size(); index++) {
                    System.out.println("#" + (index + 1) + ": " + mergedVisits.get(index).toStringWithDate());
                } // for
                */

                //write to the pre-processing results file
                ppResultsFileWriter.println("================ Merged visits of the user: "
                        + userName + " ================");
                for(int index = 0; index < mergedVisits.size(); index ++)
                    ppResultsFileWriter.println("#" + (index + 1) + ": "
                            + mergedVisits.get(index).toStringWithDate());
                //------------------------------------------------------------------------------------------


                //------------------------------------------------------------------------------------------
                //now populate pre-processed file with structured data
                for(Visit v : mergedVisits)
                {
                    String user = v.getUserName();
                    String locationName = v.getLocation().getName();
                    double latitude = v.getLocation().getLatitude();
                    double longitude = v.getLocation().getLongitude();
                    //getTime() returns: the number of milliseconds since January 1, 1970,
                    //00:00:00 GMT represented by the given date;
                    long unixTimeStampInMilliseconds = v.getTimeSeriesInstant().getArrivalTime().getTime();
                    long residenceTimeInSeconds = v.getTimeSeriesInstant().getResidenceTimeInSeconds();

                    //one line will be comma-separated list of values;
                    //therefore, they will be easily splittable by String.split() method later
                    ppUserFileWriter.println(user + "," + locationName + "," + latitude + "," + longitude + ","
                            + unixTimeStampInMilliseconds + "," + residenceTimeInSeconds);
                } // for
                //------------------------------------------------------------------------------------------

            } // try
            catch(IOException ioex)
            {
                ioex.printStackTrace();
            } // catch
            finally
            {
                //close print writers
                if(ppUserFileWriter != null)
                    ppUserFileWriter.close();

                if(ppResultsFileWriter != null)
                    ppResultsFileWriter.close();
            } // finally

        } // for


        return ppFilePaths;
    } // generatePPUserFilePaths


    //helper method to filter out visits to significant locations
    public static ArrayList<Visit> getVisitsToSignificantLocations(JavaSparkContext sc, Dataset dataset,
                                                                   String keyFileName,  ArrayList<Visit> mergedVisits,
                                                                   double significantLocationThreshold, double sigma,
                                                                   Double eps, Integer minPts) // DBSCAN parameters
    {
        //for getting original visits before significant location extraction
        //return mergedVisits;

        ///*
        switch(dataset)
        {
            //Applying 2-D gaussian distribution weighted by residence time in seconds
            case Cabspotting:
            case CenceMeGPS:
            {
                DistanceMeasure dm = new DistanceMeasure()
                {
                    private static final long serialVersionUID = -7722239876260303382L;

                    //Compute the distance between two n-dimensional vectors.
                    @Override
                    public double compute(double[] point1, double[] point2)
                    {
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


                /*
                HashSet<Location> uniqueGPSPoints = new HashSet<Location>();
                for(Visit v : mergedVisits)
                {
                    uniqueGPSPoints.add(v.getLocation());
                } // for

                //System.out.println("Number of merged visits = " + mergedVisits.size());
                //System.out.println("Number of unique locations = " + uniqueGPSPoints.size());


                Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits);
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
                } // for
                visitHistories.clear(); visitHistories = null;


                //location and weighted cumulative gaussian map, of the given muLocation with respect to other locations
                HashMap<Location, Double> gpsPointAndWeightedCumulativeGaussianMap = new HashMap<Location, Double>();

                //for each unique location, find its weighted cumulative gaussian value with respect to other locations;
                //we also calculate gaussian value of location with respect to itself
                for (Location muLocation : uniqueGPSPoints)
                {
                    double weightedCumGaussian_F_mu_l_hat = 0;

                    //calculate F_mu_l_hat = sum{ol from uniqueGPSPoints} F_mu_l(ol) * w_ol / W
                    for (Location otherLocation : uniqueGPSPoints)
                    {
                        long totalResSecondsOfOtherLocation_w_ol = locationAndTotalResSecondsOfThisUser_l_w_l_Map.get(otherLocation);
                        double gaussian_F_mu_l_ol = Utils.gaussian(muLocation, otherLocation, sigma); //F_mu_l(ol)
                        weightedCumGaussian_F_mu_l_hat += gaussian_F_mu_l_ol
                                // multiplication by weight
                                * totalResSecondsOfOtherLocation_w_ol
                                / (1.0 * totalResSecondsOfAllVisitsOfThisUser_W);
                    } // for

                    //update the map
                    gpsPointAndWeightedCumulativeGaussianMap.put(muLocation, weightedCumGaussian_F_mu_l_hat);
                } // for


                //clear memory
                locationAndTotalResSecondsOfThisUser_l_w_l_Map.clear(); locationAndTotalResSecondsOfThisUser_l_w_l_Map = null;
                */


                //gpsPointAndWeightedCumulativeGaussianMap are constructed from merged visits
                //therefore to obtain gaussianized visits, serialized weighted cumulative gaussian maps can be used
                Map<Location, Double> gpsPointAndWeightedCumulativeGaussianMap
                        = Utils.correspondingTuple(dataset, keyFileName, false);


                //the same location can be considered more than once in the clustering
                ArrayList<Location> aboveThresholdGPSPointsOfThisUser = new ArrayList<>();

                //for each visit's location, if location weighted cumulative Gaussian value is above the threshold,
                //add it to the list
                for(Visit v : mergedVisits)
                {
                    Location gpsPoint = v.getLocation();
                    double aggregatedWeightedGaussian = gpsPointAndWeightedCumulativeGaussianMap.get(gpsPoint);

                    //if \hat_{F_\mu} > T or \hat_{F_\mu} == T
                    if(aggregatedWeightedGaussian > significantLocationThreshold
                            || Double.compare(aggregatedWeightedGaussian, significantLocationThreshold) == 0)
                    {
                        aboveThresholdGPSPointsOfThisUser.add(gpsPoint);
                    } // if
                } // for




                //CLUSTERING START...
                DBSCANClusterer<Location> clusterer
                        = new DBSCANClusterer<Location>(eps, minPts, dm);

                //cluster aboveThresholdGPSPointsOfThisUser
                List<Cluster<Location>> clusters = clusterer.cluster(aboveThresholdGPSPointsOfThisUser);

                //remove small clusters
                clusters.removeIf(locationCluster -> locationCluster.getPoints().size() < minPts + 1);

                //list of all clustered points
                //List<Location> gpsPointsInClusters = new ArrayList<>();

                //map to hold gps point and its cluster id
                HashMap<Location, Integer> gpsPointClusterIDMap = new HashMap<>();
                HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();

                //lastClusterID variable for each obtained cluster
                int lastClusterID = 1;

                //to save memory we will remove clusters one by one through iterator after processing
                Iterator<Cluster<Location>> clusterIterator = clusters.iterator();

                //now for each cluster update list and mp
                //for(Cluster<Location> cluster : clusters)
                while(clusterIterator.hasNext())
                {
                    List<Location> gpsPointsInThisCluster = clusterIterator.next().getPoints();
                    //gpsPointsInClusters.addAll(gpsPointsInThisCluster);


                    //now populate location and cluster id map
                    for(Location gpsPoint : gpsPointsInThisCluster)
                    {
                        //if there are the same points, they will be in the same cluster, therefore hash map is not affected
                        gpsPointClusterIDMap.put(gpsPoint, lastClusterID);
                    } // for

                    Location averageLocationForThisCluster = Utils.averageLocation(gpsPointsInThisCluster);
                    clusterAverageLocationMap.put(lastClusterID, averageLocationForThisCluster);

                    //remove the cluster
                    clusterIterator.remove();


                    //increment clusterID for next cluster
                    lastClusterID ++;
                } // for
                clusters = null;



                //handle that carefully
                //add outlierGPSPoints to be in their single point clusters
                //aboveThresholdGPSPointsOfThisUser.removeAll(gpsPointsInClusters);

                //HashSet<Location> outlierGPSPoints
                //        = aboveThresholdGPSPointsOfThisUser;

                //List<Location> outlierGPSPoints = aboveThresholdGPSPointsOfThisUser;


                //do not add outlier above threshold gps points as clusters
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



                //gpsPointsInClusters.clear(); gpsPointsInClusters = null;



                //index starts from 1
                //int clusterIndex = 1; // is the same as lastClusterID, will the value of lastClusterID at the end of below for loop

                //map to hold cluster id and its average location
                //HashMap<Integer, Location> clusterAverageLocationMap = new HashMap<>();

                //for each cluster, compute its average location and store it in a map
                //for(Cluster<Location> cluster : clusters)
                //{
                //    Location averageLocationForThisCluster = Utils.averageLocation(cluster.getPoints());
                //    clusterAverageLocationMap.put(clusterIndex, averageLocationForThisCluster);
                //
                //    //increment cluster index
                //    clusterIndex ++;
                //} // for



                //do not add each outlier GPS point to the nearest cluster
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



                //list to store significant visits
                ArrayList<Visit> gaussianizedVisits = new ArrayList<>();

                //remove processed visit from merged visits by iterator
                Iterator<Visit> mergedVisitsIterator = mergedVisits.iterator();

                //for each visit, if visit is significant, then reroute it its corresponding significant location
                //for(Visit v : mergedVisits)
                while(mergedVisitsIterator.hasNext())
                {
                    Visit v = mergedVisitsIterator.next();

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
                    } // if


                    //remove processed visit
                    mergedVisitsIterator.remove();

                } // for
                mergedVisits = null;
                gpsPointClusterIDMap.clear(); gpsPointClusterIDMap = null;
                clusterAverageLocationMap.clear(); clusterAverageLocationMap = null;


                return gaussianizedVisits;
            } // case Cabspotting or CenceMeGPS
            case DartmouthWiFi:
            case IleSansFils:
            {
                //We define an access point as a significant place for a certain user if this user
                //has a sequence of at least n visits to the access point, in order to filter out
                //all the access points that are seldom visited and to have a sufficient number of
                //observations from a statistical point of view. For the analysis
                //presented in this paper, we select n equal to 20.

                //obtain unique locations before pre-processing
                //HashSet<Location> uniqueLocations = new HashSet<Location>();
                //for(Visit v : mergedVisits)
                //{
                //    uniqueLocations.add(v.getLocation());
                //} // for


                //System.out.println("Number of merged visits = " + mergedVisits.size());
                //System.out.println("Number of unique locations = " + uniqueLocations.size());


                //-------------------- OLD --------------
                ////specific significant location extraction method for DartmouthWiFi dataset
                ////final visits after visit frequency threshold based pre-processing
                //TreeSet<Visit> finalMergedVisits = new TreeSet<Visit>();

                ////significant unique locations
                ////HashSet<Location> significantUniqueLocations = new HashSet<Location>();

                ////create visit histories for all merged visits, even for locations which are visited only once
                //Collection<VisitHistory> visitHistories = Utils.createVisitHistories(mergedVisits);
                //for(VisitHistory vh : visitHistories)
                //{
                //    if(vh.getFrequency() >= significantLocationThreshold)
                //    {
                //        finalMergedVisits.addAll(vh.getVisits());
                //        //significantUniqueLocations.add(vh.getLocation());
                //    } // if
                //} // for
                ////clear memory
                //visitHistories.clear(); visitHistories = null;


                ////System.out.println("Number of significant (unique) locations = " + significantUniqueLocations.size());
                ////System.out.println("Number of final merged visits = " + finalMergedVisits.size());

                //return new ArrayList<Visit>(finalMergedVisits);
                //-------------------- OLD --------------



                //elementSet() returns the set of distinct elements contained in a multiset.
                //hash multi set count is performed in O(1) time -> very efficient.
                HashMultiset<Location> locationHashMultiset = Utils.locationHashMultiset(mergedVisits);
                ArrayList<Visit> finalMergedVisits = new ArrayList<Visit>();
                //for each visit, check whether its location' frequency (count) is above or equal to threshold
                for(Visit v : mergedVisits)
                {
                    if(locationHashMultiset.count(v.getLocation()) >= (int) significantLocationThreshold)
                        finalMergedVisits.add(v);
                } // for
                return finalMergedVisits;
            } // case DartmouthWiFi or IleSansFils
            default:
                return null;
        } // switch
        //*/

    } // getVisitsToSignificantLocations


    //helper method to get the list of user raw data file paths
    public static ArrayList<String> getUserRawDataFilePaths(String pathOfXmlFileContainingUserFileNames,
                                                            Dataset dataset)
    {
        //pre-processed user file paths
        ArrayList<String> userRawDataFilePaths = new ArrayList<String>();

        //read the xml file and get file names from it and pre-process them
        try
        {
            //e.g. sorted _cabs.xml file should have the structure of the following:
            //<cabs>
            //      <cab id="abboip" updates="22717"/>
            //       ...
            //</cabs>
            File sortedXmlFile = new File(pathOfXmlFileContainingUserFileNames);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(sortedXmlFile);

            //NodeList nodeList = doc.getElementsByTagName("cab");
            NodeList nodeList = doc.getElementsByTagName(dataset.userTagName());
            List<Node> nodes = new ArrayList<Node>();
            for (int index = 0; index < nodeList.getLength(); index++) {
                nodes.add(nodeList.item(index));
            } // for


            //array list to hold found file names;
            //because findFileName() can have multiple results
            //this array list will ensure that the same file is not found twice or more
            List<String> foundFileNames = new ArrayList<String>();


            //traverse through nodes, extract file names
            for (Node node : nodes)
            {
                //String userName = ((Element) node).getAttribute("id");
                String userName = ((Element) node).getAttribute(dataset.userAttributeName()); //+ ".txt"; => when .equals() is used findFileByName()
                File foundFile = findFileByName(new File(sortedXmlFile.getParent() /*"resources"*/),
                        userName);  // real file names are actually derived from user names


                //if the file is found
                if (foundFile != null)
                {
                    //--------------------------------------------
                    // the file is found; we can do pre-processing
                    //--------------------------------------------

                    //get the file name (which also includes its extension)
                    String foundFileName = foundFile.getName();

                    //check whether we have processed the file already; if we have, then continue the loop
                    if (foundFileNames.contains(foundFileName)) continue;
                    else
                    {
                        //the file is new, update the foundFileNames array list
                        foundFileNames.add(foundFileName);

                        //also update absolute paths array list of found file name
                        userRawDataFilePaths.add(foundFile.getAbsolutePath());
                    } // else

                } // if
            } // for

        } // try
        catch(Exception ex)
        {
            ex.printStackTrace();

            //exception occured return empty list
            return new ArrayList<String>();
        } // catch


        //return the resulting list
        return userRawDataFilePaths;
    } // getUserRawDataFilePaths


    //helper method to print original (un-preprocessed) visits from raw file
    public static void printSortedOriginalVisitsFromRawFile(String userRawDataFilePath, Dataset dataset)
    {
        String fileNameWithExtension  = FilenameUtils.getName(userRawDataFilePath);
        String fileExtension = FilenameUtils.getExtension(userRawDataFilePath);
        String userName = fileNameWithExtension.replace(fileExtension, "");

        //get the sorted visits
        ArrayList<Visit> originalUnMergedVisits = new ArrayList<Visit>(generateSortedOriginalVisitsFromRawFile(userName, userRawDataFilePath, dataset));
        System.out.println("======= Original unmerged visits from " + "\"" + fileNameWithExtension + "\"" + " file =======");
        for(int index = 0; index < originalUnMergedVisits.size(); index ++)
        {
            System.out.println("#" + (index + 1) + ": " + originalUnMergedVisits.get(index).toStringWithDate());
        } // for
    } // printSortedOriginalVisitsFromRawFile


    //helper method which returns original (un-preprocessed) visits from raw file
    public static TreeSet<Visit> sortedOriginalVisitsFromRawFile(String userRawDataFilePath, Dataset dataset)
    {
        String fileNameWithExtension  = FilenameUtils.getName(userRawDataFilePath);
        String fileExtension = "." + FilenameUtils.getExtension(userRawDataFilePath);
        String userName = fileNameWithExtension.replace(fileExtension, "");

        //return original (un-preprocessed) visits
        return generateSortedOriginalVisitsFromRawFile(userName, userRawDataFilePath, dataset);
    } // sortedOriginalVisitsFromRawFile


    //helper method to get sorted original visits from file
    private static TreeSet<Visit> generateSortedOriginalVisitsFromRawFile(String userName,
                                                                          String userRawDataFilePath, Dataset dataset)
    {
        //empty TreeSet visits to be filled from file
        //TreeSet will be sorted based Visit's natural order which is TimeSeriesInstant order (in turn Date order)
        TreeSet<Visit> visits = new TreeSet<Visit>();

        //we will perform different pre-processing techniques for each dataset
        switch (dataset)
        {
            case Cabspotting:
                //initially null, to be closed in finally block
                BufferedReader bReader = null;

                try
                {
                    //single line of the document read
                    String singleLine = null;

                    //initialize buffered reader
                    bReader = new BufferedReader(new FileReader(new File(userRawDataFilePath)));

                    //go through all the lines of the file
                    while ((singleLine = bReader.readLine()) != null)
                    {
                        //split string and create respective objects
                        String[] singleLineComponents = singleLine.split(" ");
                        double latitude = Double.parseDouble(singleLineComponents[0]);
                        double longitude = Double.parseDouble(singleLineComponents[1]);
                        //Cabspotting dataset interestingly contains the seconds from the Unix epoch,
                        //instead of milliseconds
                        long unixTimeStampInSeconds = Long.parseLong(singleLineComponents[3]);
                        //* 1000 will ensure milliseconds
                        Date timeStamp = new Date(unixTimeStampInSeconds * 1000);

                        //System.out.println(timeStamp);

                        //4 decimal places decreases the accuracy, we keep latitude and longitude as it is
                        //latitude = Utils.format(latitude , "#.#####", RoundingMode.FLOOR);
                        //longitude = Utils.format(longitude , "#.#####", RoundingMode.FLOOR);


                        //create objects;
                        //location name is not available in the original dataset,
                        //new Location(latitude, longitude) will create a location with a name UNK
                        Location location = new Location(latitude, longitude);
                        TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(timeStamp, 0);
                        Visit visit = new Visit(userName, location, timeSeriesInstant);
                        visits.add(visit);
                    } // while
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

                //sort visits list by visit Date
                //Collections.sort(visits); //already using TreeSet

                //break switch statement
                break;
            case CenceMeGPS:
                //create file from user's raw data file path
                File file = new File(userRawDataFilePath);
                String gpsMatching = "GPS:";
                String gpsSkippedMatching = "GPS-Skipped:";
                String nextLogMatching = "NEXT LOG";
                Scanner scanner = null;


                //hash map to hold nexLogCount and string for that log
                HashMap<Integer, String> nextLogSbMap = new HashMap<>();
                try
                {
                    //obtain scanner for a file
                    scanner = new Scanner(file);

                    //next log count to distinguish first NEXT LOG from others
                    int nextLogCount = 0;

                    //flag for the case if next log is found
                    boolean isNextLogFound = false;

                    //string builder to hold each log
                    StringBuilder sb = new StringBuilder("");

                    //now read the file line by line
                    while (scanner.hasNextLine())
                    {
                        //read the file line by line
                        String line = scanner.nextLine();

                        if(line.contains(nextLogMatching))
                        {
                            isNextLogFound = true;

                            if(nextLogCount != 0)
                            {
                                String thisLog = sb.toString();

                                //it is possible that after "NEXT LOG", no "GPS:" or "GPS-Skipped" lines are found
                                //which makes sb empty;
                                //we add sb to the map if it is non-empty;
                                //on the other hand, thisLog should contain at least one GPS: matching with samples
                                //to have a real visit;
                                if(!thisLog.equals("") && thisLog.contains(gpsMatching))
                                {
                                    nextLogSbMap.put(nextLogCount, sb.toString());
                                } // if
                            } // if

                            nextLogCount ++;
                            sb = new StringBuilder("");
                        } // if

                        if(isNextLogFound)
                        {
                            if(line.contains(gpsMatching) || line.contains(gpsSkippedMatching))
                            {
                                sb.append(line).append("\n");
                            } // if
                        } // if

                    } // while

                    //after final nextLogMatching there is no NEXT LOG to add the hash map
                    //therefore, we add after we reach the end of a file;
                    //final nextLogCount is already available after while loop above;
                    //it is possible that after "NEXT LOG", no "GPS:" or "GPS-Skipped" lines are found; which makes sb empty
                    //we add sb to the map if it is non-empty;
                    //on the other hand, thisLog should contain at least one GPS: matching with samples
                    //to have a real visit;
                    String thisLog = sb.toString();
                    if(!thisLog.equals("") && thisLog.contains(gpsMatching))
                    {
                        nextLogSbMap.put(nextLogCount, sb.toString());
                    } // if

                } //try
                catch(IOException ioex)
                {
                    ioex.printStackTrace();
                } // catch
                finally
                {
                    //close when the job finished
                    if(scanner != null)
                        scanner.close();
                } // finally


                //System.out.println("userName: " + userName + "; logCount: " + nextLogSbMap.keySet().size());
                //System.out.println(nextLogSbMap);
                //for each next log in the hash map, generate visits, separate them with dummy visit and add all to visits tree set
                for(Integer logID : nextLogSbMap.keySet())
                {
                    String log = nextLogSbMap.get(logID);
                    TreeSet<Visit> thisLogVisits = new TreeSet<>();
                    //System.out.println("\n\nlogCount: " + nextLogSbMap.keySet().size());
                    //System.out.println("this log: \n" + log);
                    //System.exit(0);


                    //try
                    //{

                    //obtain scanner for this log //file
                    scanner = new Scanner(log); //(file);

                    //line count for matchings
                    //int lineCount = 1;

                    //now read the file line by line
                    while (scanner.hasNextLine())
                    {
                        //read the file line by line
                        String line = scanner.nextLine();

                        //apply different pre-processing techniques for each matching
                        if (line.contains(gpsMatching))
                        {
                            //IF SENSOR READINGS ARE NOT CHRONOLOGICALLY ORDERED IN THE DATASET
                            //DEFINED TREE SET WILL DO IT FOR US

                            //System.out.println("#" + lineCount + ": " + line);
                            //increment line count
                            //lineCount ++;

                            //one line can contain the following:
                            //1216873771895 DATA (0) - GPS: 189.3,4341.43106,7217.43544,2.7,0.0*
                            String[] lineComponents = line.split(" ");

                            //index 0 is unix "timeStamp" which is the time
                            //when the line has been written into the log file;
                            long unixTimeStampInMilliseconds = Long.parseLong(lineComponents[0]);
                            Date gpsTimeStamp = new Date(unixTimeStampInMilliseconds);
                            //System.out.println("\ttimeStamp: " + gpsTimeStamp);


                            //if we have a line where GPS: without samples comes before GPS: with samples
                            //and we do not have at least one visit in visits tree set
                            //then "continue" while loop for other sensor readings
                            if (lineComponents.length < 6 && thisLogVisits.size() == 0) //visits.size() == 0)
                                continue; // continue while loop, if continue executes, below lines will not be executed


                            //In the dataset we also have GPS: lines without samples
                            //in the case of GPS: with no samples, length of lineComponents becomes 5,
                            //without having the index 5 of
                            //altitude,latitude,longitude,hdop,speed*altitude,latitude,longitude ,hdop,speed*……….
                            //therefore, for this type lines we will use last known position as we did in GPS-Skipped:
                            if (lineComponents.length < 6)
                            {
                                //we do not have GPS samples, so assume last know location
                                //at this point we will have at least one visit
                                //contained in visits tree set

                                //System.err.println("GPS with no samples => timeStamp: " + unixTimeStampInMilliseconds + ", userName: " + userName);

                                //-------------- NEW ---------------
                                ////replicate last know location
                                //Location lastLocation = visits.last().getLocation();
                                //TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(gpsTimeStamp, 0);
                                //Visit visit = new Visit(userName, lastLocation, timeSeriesInstant);
                                //thisLogVisits.add(visit); //visits.add(visit);
                                //-------------- NEW ---------------


                                //-------------- OLD ---------------
                                //we will update the residence time of last added visit
                                Visit lastVisit = thisLogVisits.last(); //visits.last(); //get(visits.size() - 1);
                                Date lastVisitArrivalTime = lastVisit.getTimeSeriesInstant().getArrivalTime();

                                //obtain number of seconds between two gps readings' timeStamps
                                //and update last visit residence time with it
                                long noOfSecondsBetweenGPSReadings = Utils.noOfSecondsBetween(lastVisitArrivalTime,
                                        gpsTimeStamp);
                                lastVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(noOfSecondsBetweenGPSReadings);
                                //-------------- OLD ---------------
                            } // if
                            else        //the line is GPS: with samples
                            {
                                //index 5 consists of altitude,latitude,longitude,hdop,speed*altitude,latitude,longitude ,hdop,speed*……….
                                //one more split operation by "," should be performed on index 5 to extract "latitude" and "longitude"
                                //or maybe the altitude


                                //--------- NEW ------------
                                ////Average latitude and longitude of N samples separated by *
                                ////1. Split by *
                                ////2. Then split by ,
                                ////3. average latitudes and longitudes
                                //String[] gpsSamples = lineComponents[5].split("\\*"); // * is a metacharacter, it must be escaped
                                ////e.g. 171.4,4341.44336,7217.40298,2.5,0.0* will be splitted as
                                ////0 -> "171.4,4341.44336,7217.40298,2.5,0.0"
                                ////the part after * will not be in the string array above
                                //double sumOfLatitudes = 0;
                                //double sumOfLongitudes = 0;

                                ////loop over all samples and average their latitudes and longitudes
                                //for(String gpsSample : gpsSamples)
                                //{
                                //    //now again split by "," and then average the latitudes and longitudes
                                //    //NMEALatitude will be at index 1
                                //    //NMEALongitude will be at index 2
                                //    String[] gpsReadings = gpsSample.split(",");
                                //    //for South and West, multiplication by -1 is necessary, since dataset does not provide negative NMEA value
                                //    //most locations are around the Dartmouth College, which is North, West
                                //    double NMEALatitude = Double.parseDouble(gpsReadings[1]); //North
                                //    double NMEALongitude = -1 * Double.parseDouble(gpsReadings[2]); //West, multiply it by -1
                                //
                                //    //convert NMEA messages to real latitude and longitude values
                                //    double latitude = Utils.NMEADecimalToDecimal(NMEALatitude);
                                //    double longitude = Utils.NMEADecimalToDecimal(NMEALongitude);
                                //
                                //    //sum up the latitudes and longitudes
                                //    sumOfLatitudes += latitude;
                                //    sumOfLongitudes += longitude;
                                //} // for

                                //now calculate the average latitude and longitude
                                //double finLatitude = Utils.format(sumOfLatitudes / gpsSamples.length, "#.#####");
                                //                    //sumOfLatitudes / gpsSamples.length;
                                //double finLongitude = Utils.format(sumOfLongitudes / gpsSamples.length, "#.#####");
                                //                    //sumOfLongitudes / gpsSamples.length;
                                //--------- NEW ------------


                                //--------- OLD ------------
                                String[] gpsReadings = lineComponents[5].split(",");

                                //latitude at index 1
                                //longitude at index 2
                                //for South and West, multiplication by -1 is necessary, since dataset does not provide negative NMEA value
                                //most locations are around the Dartmouth College, which is North, West
                                double NMEALatitude = Double.parseDouble(gpsReadings[1]); //North
                                double NMEALongitude = -1 * Double.parseDouble(gpsReadings[2]); //West, multiply it by -1

                                //convert NMEA messages to real latitude and longitude values
                                double finLatitude = Utils.NMEADecimalToDecimal(NMEALatitude); //Utils.NMEADecimalToDecimal(NMEALatitude, "#.#####", RoundingMode.FLOOR);
                                double finLongitude = Utils.NMEADecimalToDecimal(NMEALongitude); //Utils.NMEADecimalToDecimal(NMEALongitude, "#.#####", RoundingMode.FLOOR);
                                //--------- OLD ------------


                                //create objects;
                                //location name is not available in the original dataset,
                                //new Location(latitude, longitude) will create a location with a name UNK
                                Location location = new Location(finLatitude, finLongitude);
                                TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(gpsTimeStamp, 0);
                                Visit visit = new Visit(userName, location, timeSeriesInstant);
                                thisLogVisits.add(visit); //visits.add(visit);
                            } // else
                        } // if
                        else if (line.contains(gpsSkippedMatching))
                        {
                            //IF SENSOR READINGS ARE NOT CHRONOLOGICALLY ORDERED IN THE DATASET
                            //DEFINED TREE SET WILL DO IT FOR US

                            //System.out.println("#" + lineCount + ": " + line);
                            //increment line count
                            //lineCount ++;

                            //one line can contain the following:
                            //1216914124468 DATA (0) - GPS-Skipped: user sitting
                            String[] lineComponents = line.split(" ");

                            //we will only need timestamp here which is index 0
                            //index 0 is unix "timeStamp" which is the time
                            //when the line has been written into the log file;
                            long unixTimeStampInMilliseconds = Long.parseLong(lineComponents[0]);
                            Date gpsSkippedTimeStamp = new Date(unixTimeStampInMilliseconds);

                            //System.out.println("\ttimeStamp: " + gpsSkippedTimeStamp);

                            //if we have a line where GPS-Skipped: comes before GPS:
                            //and we do not have at least one visit in tree set
                            //then "continue" while loop for other sensor readings
                            if (thisLogVisits.size() == 0) //(visits.size() == 0)
                                continue; // continue while loop, if continue execute below lines will not be executed


                            //-------------- NEW ---------------
                            ////replicate last know location
                            //Location lastLocation = visits.last().getLocation();
                            //TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(gpsSkippedTimeStamp, 0);
                            //Visit visit = new Visit(userName, lastLocation, timeSeriesInstant);
                            //thisLogVisits.add(visit); //visits.add(visit);
                            //-------------- NEW ---------------


                            //-------------- OLD ---------------
                            //at this point we will have at least one visit
                            //contained in visits tree set
                            //GPS-Skipped: case was designed for power issues: every time a user is sitting for more
                            //than 15 minutes the GPS sensor is not used. These lines can be replaced using the last
                            //known position; Therefore we will update the residence time of last added visit
                            Visit lastVisit = thisLogVisits.last(); //visits.last(); //get(visits.size() - 1);
                            Date lastVisitArrivalTime = lastVisit.getTimeSeriesInstant().getArrivalTime();

                            //obtain number of seconds between two gps readings' timeStamps
                            //and update last visit residence time with it
                            long noOfSecondsBetweenGPSReadings = Utils.noOfSecondsBetween(lastVisitArrivalTime,
                                    gpsSkippedTimeStamp);
                            lastVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(noOfSecondsBetweenGPSReadings);
                            //-------------- OLD ---------------
                        } // else if
                    } // while



                    //-------------- NEW PRE-PROCESSING ---------------
                    //add a dummy visit to separate logs from each other
                    //dummy visit will be 1 second after last final visit and have a residence seconds of 0
                    //and a location of DUMMY,  91 (MAX_LATITUDE + 1), -181 (MIN_LONGITUDE - 1)
                    //adding 1 second to last final visit will gaurantee that dummy visit will be last visit in the tree set
                    Visit lastFinalVisit = thisLogVisits.last();
                    Date lastFinalVisitArrivalTime = lastFinalVisit.getTimeSeriesInstant().getArrivalTime();
                    long lastFinalVisitResSeconds = lastFinalVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();
                    Date dummyVisitArrivalTime = Utils.addSeconds(lastFinalVisitArrivalTime, lastFinalVisitResSeconds + 1);
                    Visit dummyVisit = new Visit(userName, new Location("DUMMY", 91, -181),
                                            new TimeSeriesInstant(dummyVisitArrivalTime, 0));
                    thisLogVisits.add(dummyVisit);

                    //now add all to visits tree set
                    visits.addAll(thisLogVisits);
                    //-------------- NEW PRE-PROCESSING ---------------



                    //} // try
                    //catch(IOException ex)
                    //{
                    //    ex.printStackTrace();
                    //} // catch
                    //finally
                    //{
                    //    //close when the job finished
                    //    if(scanner != null)
                            scanner.close();
                    //} // finally


                } // for




                //break switch statement
                break;
            case DartmouthWiFi:
                //initially null, to be closed in finally block
                BufferedReader rawFileReader = null;

                try
                {
                    //single line of the document read
                    String singleLine = null;

                    //initialize buffered reader
                    rawFileReader = new BufferedReader(new FileReader(new File(userRawDataFilePath)));

                    //go through all the lines of the file
                    while((singleLine = rawFileReader.readLine()) != null)
                    {
                        //split string and create respective objects
                        String[] singleLineComponents = singleLine.split("\\t");
                        //DartmouthWiFi dataset interestingly contains the seconds from the Unix epoch,
                        //instead of milliseconds
                        long unixTimeStampInSeconds = Long.parseLong(singleLineComponents[0]);
                        //* 1000 will ensure milliseconds
                        Date timeStamp = new Date(unixTimeStampInSeconds * 1000);

                        //index 1 contains location name, we do not need it in the creation of the visit yet
                        String locationName = singleLineComponents[1];

                        //------------- OLD ---------------
                        double latitude = Double.parseDouble(singleLineComponents[2]);
                        double longitude = Double.parseDouble(singleLineComponents[3]);
                        //------------- OLD ---------------


                        //NO NEED SINCE GAUSSIAN PRE-PROCESSING DOES NOT WORK FOR DartmouthWiFi DATASET
                        //------------- NEW ---------------
                        //double DMSLatitude = Double.parseDouble(singleLineComponents[2]);
                        //double DMSLongitude = Double.parseDouble(singleLineComponents[3]);
                        //double latitude = Utils.DMSToDecimal(DMSLatitude);
                        //double longitude = Utils.DMSToDecimal(DMSLongitude);
                        //------------- NEW ---------------



                        //System.out.println(timeStamp);



                        //------------- OLD ---------------
                        //if location name is OFF, use the last known location
//                        if(locationName.equals("OFF"))
//                        {
//                            //if OFF comes before anything, then continue the loop for the real location
//                            if(visits.size() == 0) continue;
//
//                            //at this point, we already have one visit in visits tree set
//                            Visit lastVisit = visits.last();
//                            Date lastVisitArrivalTime = lastVisit.getTimeSeriesInstant().getArrivalTime();
//
//                            //obtain number of seconds between two readings' timeStamps
//                            //and update last visit residence time with it
//                            long noOfSecondsBetweenTwoReadings = Utils.noOfSecondsBetween(lastVisitArrivalTime,
//                                    timeStamp);
//                            lastVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(noOfSecondsBetweenTwoReadings);
//                        } // if
//                        else
//                        {
//                            //create objects
//                            Location location = new Location(locationName, latitude, longitude);
//                            TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(timeStamp, 0);
//                            Visit visit = new Visit(userName, location, timeSeriesInstant);
//                            visits.add(visit);
//                        } // else
                        //------------- OLD ---------------



                        //pass even OFF as a location for the visits; new mergeVisits() method will handle that
                        //------------- NEW ---------------
                        //create objects
                        Location location = new Location(locationName, latitude, longitude);
                        TimeSeriesInstant timeSeriesInstant = new TimeSeriesInstant(timeStamp, 0);
                        Visit visit = new Visit(userName, location, timeSeriesInstant);
                        visits.add(visit);
                        //------------- NEW ---------------
                    } // while
                } // try
                catch(IOException ex)
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
                } // finally

                //break switch statement
                break;
            case IleSansFils:
                //initially null, to be closed in finally block
                BufferedReader reader = null;

                try
                {
                    //single line of the document read
                    String singleLine = null;

                    //initialize buffered reader
                    reader = new BufferedReader(new FileReader(new File(userRawDataFilePath)));

                    //go through all the lines of the file
                    while ((singleLine = reader.readLine()) != null)
                    {
                        //0 -> conn_id
                        //1 -> timestamp_in  -> arrival time in "yyyy-MM-dd HH:mm:ss" format
                        //2 -> node_id       -> location
                        //3 -> timestamp_out -> sometimes NULL, in "yyyy-MM-dd HH:mm:ss" format
                        //4 -> user_id       -> user name
                        //5 -> user_mac
                        //6 -> incoming
                        //7 -> outgoing

                        String[] singleLineComponents = singleLine.split(",");

                        //obtain arrival time, end time and residence seconds
                        String dateFormatPattern = "yyyy-MM-dd HH:mm:ss";
                        String timeStampIn = singleLineComponents[1];
                        String timeStampOut = singleLineComponents[3];
                        long residenceSeconds = 0;
                        try
                        {
                            if(timeStampIn.equalsIgnoreCase("NULL"))
                            {
                                System.err.println("timestamp_in is NULL");
                                continue; // then this line should be discarded
                            } //if

                            //arrival time can be generated, at this point
                            Date arrivalTime = Utils.toDate(timeStampIn, dateFormatPattern);

                            //if end time is non-null; update residence seconds
                            if(!timeStampOut.equalsIgnoreCase("NULL"))
                            {
                                Date endTime = Utils.toDate(timeStampOut, dateFormatPattern);
                                residenceSeconds = Utils.noOfSecondsBetween(arrivalTime, endTime);
                            } // if


                            //user name;  WE DO NOT USE userName PARAMETER PASSED TO THIS METHOD
                            String userID = singleLineComponents[4];
                            if(userID.equals("")) throw new RuntimeException("User id is empty string");


                            //location name
                            String locationName = singleLineComponents[2];
                            //default latitude and longitude is -1, because only anonymized wifi access point names are given
                            Location loc = new Location(locationName, -1, -1);


                            //create new visit and add it to the visits set
                            Visit newVisit = new Visit(userID, loc, new TimeSeriesInstant(arrivalTime, residenceSeconds));
                            visits.add(newVisit);
                        } catch (ParseException e)
                        {
                            e.printStackTrace();
                        }

                    } // while
                } // try
                catch(IOException ioex)
                {
                    ioex.printStackTrace();
                } // catch
                finally
                {
                    try
                    {
                        if (reader != null)
                            reader.close();
                    } // try
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    } // catch
                } // finally
            break;

            default:
            break;
        } // switch


        //return pre-processed visits
        return visits;
    } // generateSortedOriginalVisitsFromRawFile


    //mergeVisitsWithDateCalculation() is easier to understand than mergeVisits()
    //helper method to merge visits if t_i+1 - (t_i + d_i) <= delta, based on Date calculation
    public static ArrayList<Visit> mergeVisitsWithDateCalculation(ArrayList<Visit> visits, int deltaThresholdInSeconds)
    {
        //visits in "visits" array list should belong to the same user, otherwise error will be thrown;
        //visits in "visits" array list should be in chronological order or Date order
        //otherwise the below can cause unexpected results
        //since we do not perform Date based comparison, if unsorted visits array list
        //is given, then one visit can be merged with visit which is Date.before() it;
        //the correct way is to merge current visit with the next visit which is Date.after() it
        //Collections.sort(visits); //visits are sorted before mergeVisits() call, therefore we do not call this

        ArrayList<Visit> mergedVisits = new ArrayList<Visit>();
        //System.out.println("Number of merged visits: " + mergedVisits.size());

        //add the original visit at index 0 as the starting point for merged visits
        mergedVisits.add(Visit.newInstance(visits.get(0)));

        //for loop to achieve merge operation
        for(int index = 1; index < visits.size(); index ++)
        {
            //nextVisit and lastMergedVisit
            Visit nextVisit = visits.get(index);
            Visit lastMergedVisit = mergedVisits.get(mergedVisits.size() - 1);

            //---------- NEW ----------
            //obtain locations of nextVisit and lastMergedVisit
            Location nextVisitLocation = nextVisit.getLocation();
            Location lastMergedVisitLocation = lastMergedVisit.getLocation();
            //---------- NEW ----------


            //get their day seconds and residence seconds and arrival times
            long lastMergedVisitResidenceSeconds = lastMergedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();
            Date nextVisitArrivalTime = nextVisit.getTimeSeriesInstant().getArrivalTime();

            Date lastMergedVisitArrivalTimePlusResidenceTime
                    = new Date(lastMergedVisit.getTimeSeriesInstant().getArrivalTime().getTime()
                    + lastMergedVisitResidenceSeconds * 1000); // * 1000 to make milliseconds

            //find number of seconds between nextVisitArrivalTime and lastMergedVisitArrivalTimePlusResidenceTime
            long noOfSecondsBetween = Utils.noOfSecondsBetween(nextVisitArrivalTime,
                    lastMergedVisitArrivalTimePlusResidenceTime);

            //if difference is within interval of deltaThresholdInSeconds, then merge them
            //deltaThreshold can be 60, 180 or 300 seconds
            if(nextVisitLocation.equals(lastMergedVisitLocation) && //---------- NEW ---------- //visits should be performed to the same location
                    noOfSecondsBetween <= deltaThresholdInSeconds)
            {
                //nextVisit and lastMergedVisit are either on the same day
                //or they are in subsequent days
                Visit mergedVisit = lastMergedVisit.mergeWith(nextVisit);
                //update the last element of the array list
                mergedVisits.set(mergedVisits.size() - 1, mergedVisit);
            } // if
            else        //either locations are not the same or visits are not in the interval of deltaThresholdInSeconds
            {
                //we will not merge consecutive visits performed to the same location which are above threshold
                //---------- OLD ----------
                //assume delta is 60
                //200, 50
                //320, 30
                // =>
                //200, 50
                //320, 30
                // NOT
                //200, 120
                //320, 30
                if (lastMergedVisitResidenceSeconds == 0)
                    //update the last merged visit with noOfSecondsBetween plus lastMergedVisitResidenceSeconds
                    lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(noOfSecondsBetween); // => t_i+1 - (t_i + d_i) = t_i+1 - t_i
                                            //+ lastMergedVisitResidenceSeconds); since it is zero

                //create new visit from the original one at index "index"
                Visit newVisit = new Visit(nextVisit.getUserName(),
                        nextVisit.getLocation(), nextVisit.getTimeSeriesInstant());
                //add it to the array list
                mergedVisits.add(newVisit);
                //---------- OLD ----------



                //---------- NEW ----------
//                //it is possible that two consecutive locations are equal
//                //still merge discarding the fact that noOfSecondsBetween > deltaThresholdInSeconds
//                if (nextVisitLocation.equals(lastMergedVisitLocation))
//                {
//                    //System.err.println("Two consecutive locations: (" + nextVisitLocation + ", " + lastMergedVisitLocation
//                    //        + ") are the same"
//                    //        + " and noOfSecondsBetween: " + noOfSecondsBetween + " > " + "deltaThresholdInSeconds: " + deltaThresholdInSeconds
//                    //        + " which results in generation of two consecutive visits to the same location");
//
//
//
//                    //still merge them despite of the fact that noOfSecondsBetween > deltaThresholdInSeconds;
//                    //nextVisit and lastMergedVisit are either on the same day
//                    //or they are in subsequent days
//                    Visit mergedVisit = lastMergedVisit.mergeWith(nextVisit);
//                    //update the last element of the array list
//                    mergedVisits.set(mergedVisits.size() - 1, mergedVisit);
//                } // if
//                else    //locations are not same and noOfSecondsBetween > deltaThresholdInSeconds
//                {
//                    //update the last merged visit with noOfSecondsBetween plus lastMergedVisitResidenceSeconds
//                    lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(noOfSecondsBetween
//                            + lastMergedVisitResidenceSeconds);
//                    //create new visit from the original one at index "index"
//                    Visit newVisit = new Visit(nextVisit.getUserName(),
//                            nextVisit.getLocation(), nextVisit.getTimeSeriesInstant());
//                    //add it to the array list
//                    mergedVisits.add(newVisit);
//                } // else
                //---------- NEW ----------

            } // else
        } // for

        //return the array list
        return mergedVisits;
    } // mergeVisitsWithDateCalculation


    //helper method to progressively merge visits for specific datasets
    //METHOD assumes that visits in the given array list parameter are sorted chronologically
    public static ArrayList<Visit> mergeVisits(Dataset dataset, ArrayList<Visit> visits, int deltaThresholdInSeconds)
    {
        switch (dataset)
        {
            case CenceMeGPS:
            {
                //mergedVisits which will be progressively built for each visits list
                ArrayList<Visit> mergedVisits = new ArrayList<Visit>();
                //visits list will contain al visits up to the visit whose location is "OFF"
                ArrayList<Visit> visitsList = new ArrayList<Visit>();

                //loop through all visits, and merge them iteratively
                //based on our assumption, we are looping through in chronological order
                for(int index = 0; index < visits.size(); index ++)
                {
                    String locationName = visits.get(index).getLocation().getName();

                    //if locationName is different than DUMMY, we can add this visit to the visitsList
                    if(!locationName.equalsIgnoreCase("DUMMY"))
                    {
                        visitsList.add(visits.get(index));

                        //in this case, we have to check whether we reached the last index of visits array list
                        if(index == visits.size() - 1)
                        {
                            mergedVisits.addAll(mergeVisitsWithDateCalculation(visitsList, deltaThresholdInSeconds));

                            //NO NEED to create a new visit list since the last index is reached
                            //visitsList = new ArrayList<Visit>();
                        } // if
                    } // if
                    else    // locationName is "DUMMY"
                    {
                        //here we should have at least one visit in the visitsList
                        //if visitsList is empty, then are two possible cases:
                        //1) first visit's location name in visits array list is "DUMMY"
                        //2) there are two subsequent "DUMMY" visits in the visits array list
                        //for both cases we will continue the loop
                        if(visitsList.size() == 0) continue;


                        //this version of merged visits creation will eliminate bigger residence seconds
                        //if there are holes in the sensor readings;
                        ArrayList<Visit> resultingVisits = mergeVisitsWithDateCalculation(visitsList, deltaThresholdInSeconds);

                        //if last resulting visit from mergeVisitsWithDateCalculation have 0 residence seconds
                        //add deltaThresholdSeconds to it
                        if(resultingVisits.get(resultingVisits.size() - 1).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                            resultingVisits.get(resultingVisits.size() - 1).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);


                        //add al resulting visits to merged visits
                        mergedVisits.addAll(resultingVisits);

                        //create a new visitsList for new visits sequence after "OFF" visit
                        visitsList = new ArrayList<Visit>();
                    } // else

                } // for

                //last entry in the merged visits array list can have 0 residence seconds
                //update it with sampling interval seconds
                if(mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                    mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);


                //return the resulting merged visits
                return mergedVisits;
            } // case CenceMeGPS

            case DartmouthWiFi:
            {
                //mergedVisits which will be progressively built for each visits list
                ArrayList<Visit> mergedVisits = new ArrayList<Visit>();
                //visits list will contain al visits up to the visit whose location is "OFF"
                ArrayList<Visit> visitsList = new ArrayList<Visit>();

                //loop through all visits, and merge them iteratively
                //based on our assumption, we are looping through in chronological order
                for(int index = 0; index < visits.size(); index ++)
                {
                    String locationName = visits.get(index).getLocation().getName();

                    //if locationName is different than OFF, we can add this visit to the visitsList
                    if(!locationName.equalsIgnoreCase("OFF"))
                    {
                        visitsList.add(visits.get(index));

                        //it is possible that, user trace does not contain any OFF visit
                        //e.g.
                        //1065790854	SocBldg11AP2	819247.8103	439098.1315
                        //1065790862	SocBldg11AP5	819061.8149	439083.2945
                        //1065790866	SocBldg11AP6	819242.417	439112.2367

                        //in this case, we have to check whether we reached the last index of visits array list
                        if(index == visits.size() - 1)
                        {
                            mergedVisits.addAll(mergeVisitsWithDateCalculation(visitsList, deltaThresholdInSeconds));

                            //NO NEED to create a new visit list since the last index is reached
                            //visitsList = new ArrayList<Visit>();
                        } // if
                    }
                    else    // locationName is "OFF"
                    {
                        //here we should have at least one visit in the visitsList
                        //if visitsList is empty, then are two possible cases:
                        //1) first visit's location name in visits array list is "OFF"
                        //2) there are two subsequent "OFF" visits in the visits array list
                        //for both cases we will continue the loop
                        if(visitsList.size() == 0) continue;


                        //at this point we have at least one visit in the visitsList
                        //update the residence seconds of the previous visit, since this visit is "OFF", not a real location
                        Visit previousVisit = visitsList.get(visitsList.size() - 1);
                        Date previousVisitTimeStamp = previousVisit.getTimeSeriesInstant().getArrivalTime();
                        Date thisVisitTimeStamp = visits.get(index).getTimeSeriesInstant().getArrivalTime();


                        //Perform the same merge operation as in section 3.2 of the paper,
                        //this visit is merged to previous visit which updates previous visit's residence seconds
                        //new residence seconds is the number of seconds between this time stamp and previous one
                        long t_i_Plus_1_Minus_t_i = Utils.noOfSecondsBetween(thisVisitTimeStamp, previousVisitTimeStamp);
                        //in this dataset d_i_Plus_1 is always 0, we have added this for convenience
                        long d_i_Plus_1 = visits.get(index).getTimeSeriesInstant().getResidenceTimeInSeconds();

                        //update the residence seconds of the previous visit
                        previousVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(t_i_Plus_1_Minus_t_i + d_i_Plus_1);

                        //it is possible that time stamp of OFF visit and previous visit is the same
                        //in this case; update res second with sampling interval seconds
                        //e.g.
                        //1065645405	ResBldg56AP13	819663.3311	438668.34
                        //1065645414	AdmBldg11AP1	-1.0	-1.0
                        //1065645414	OFF	-1.0	-1.0
                        if(previousVisit.getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                            previousVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);


                        //merge the visits in the visitsList and add all to the mergedVisits array list
                        //addAll() keeps the insertion order, so if visits array list is in the chronological order
                        //then mergedVisits will also be in chronological order;
                        //this version of merged visits creation will eliminate bigger residence seconds
                        //if there are holes in the sensor readings
                        mergedVisits.addAll(
                                mergeVisitsWithDateCalculation(visitsList, deltaThresholdInSeconds)
                                //mergeVisits(visitsList, deltaThresholdInSeconds)
                                );

                        //create a new visitsList for new visits sequence after "OFF" visit
                        visitsList = new ArrayList<Visit>();
                    } // else
                } // for


                //last entry in the merged visits array list can have 0 residence seconds
                //update it with sampling interval seconds
                if(mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                    mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);


                //return the resulting merged visits
                return mergedVisits;

            } // case DartmouthWiFi

            case IleSansFils:
            {
                //--------------------------------- OLD ---------------------------------
                //intermediateMergedVisits which will be progressively built for each visits list
                //ArrayList<Visit> intermediateMergedVisits = new ArrayList<Visit>();
                //visits list will contain al visits up to the visit whose location is "OFF"
                //ArrayList<Visit> visitsList = new ArrayList<Visit>();

                ////loop through all visits, and merge them iteratively
                ////based on our assumption, we are looping through in chronological order
                //for(int index = 0; index < visits.size(); index ++)
                //{
                //    long residenceSeconds = visits.get(index).getTimeSeriesInstant().getResidenceTimeInSeconds();
                //
                //    //if residence seconds is 0, then timeStampOut is NULL or visit end time is unknown
                //    if(residenceSeconds == 0)
                //    {
                //        visitsList.add(visits.get(index));
                //    }
                //    else    // res seconds is non-zero, it means this visit is already merged since, it has end time
                //    {
                //        //add this non-zero residence second visit to this visit list
                //        //along with previous possible zero res seconds visits
                //        visitsList.add(visits.get(index));
                //
                //        //now merge
                //        //two cases possible:
                //        //1. 0 res visits and this visit
                //        //2. only this visit
                //        intermediateMergedVisits.addAll(
                //                mergeVisitsWithDateCalculation(visitsList, deltaThresholdInSeconds)
                //                //mergeVisits(visitsList, deltaThresholdInSeconds)
                //        );
                //
                //        //create a new visitsList for new visits sequence after "OFF" visit
                //        visitsList = new ArrayList<Visit>();
                //    } // else
                //} // for


                //after this for loop, the following cases is possible (assume delta is 60 seconds):
                //Symbol ------ separates each visit list
                //1. only non-zero res seconds visits
                //merge operation will have no effect since visitsList will contain only one visit every time;
                //210, 30
                //-------
                //280, 20
                //-------
                //320, 40
                //2. 0 residence seconds visits and one non-zero res second visit
                //100, 0
                //200, 50
                //-------
                //270, 30
                //will be transformed to:
                //100, 100,
                //200, 50
                //-------
                //270, 30
                //as you see one more merge operation for all intermediate merged visits is required

                //return the resulting merged visits
                //return mergeVisitsWithDateCalculation(intermediateMergedVisits, deltaThresholdInSeconds);
                //--------------------------------- OLD ---------------------------------

                //loop through all visits and check whether NULL endTime visit and next visit has more than 8 hours timeStampIn interval;
                //if interval is more than 8 hours (max number of working hours in day), then set residence seconds of NULL endTime visit
                //to deltaThresholdInSeconds
                for(int index = 0; index < visits.size() - 1; index ++)
                {
                    //residenceSeconds == 0 visits are NULL endTime visits
                    if(visits.get(index).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                    {
                        double hourDifference = Utils.noOfSecondsBetween(visits.get(index).getTimeSeriesInstant().getArrivalTime(),
                                visits.get(index + 1).getTimeSeriesInstant().getArrivalTime()) / 3600.0; // division by 3600.0 to make hours

                        //if difference is more than 8 hours
                        if(hourDifference > 8)
                        {
                            visits.get(index).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);
                        } // if
                    } // if
                } // for


                //now merge them
                ArrayList<Visit> mergedVisits = mergeVisitsWithDateCalculation(visits, deltaThresholdInSeconds);

                //last entry in the merged visits array list can have 0 residence seconds
                //update it with sampling interval seconds
                if(mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                    mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);

                //return the resulting visits
                return mergedVisits;
            } // case IleSansFils
            default:
                //for the default case, call default method
                ArrayList<Visit> mergedVisits = mergeVisitsWithDateCalculation(visits, deltaThresholdInSeconds);

                //last entry in the merged visits array list can have 0 residence seconds
                //update it with sampling interval seconds
                if(mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                    mergedVisits.get(mergedVisits.size() - 1).getTimeSeriesInstant().setResidenceTimeInSeconds(deltaThresholdInSeconds);

                //return the resulting visits
                return mergedVisits;
        } // switch
    } // mergeVisits


    //helper method to merge visits if t_i+1 - (t_i + d_i) <= delta
    public static ArrayList<Visit> mergeVisits(ArrayList<Visit> visits, int deltaThresholdInSeconds)
    {
        //visits in "visits" array list should be in chronological order or Date order
        //otherwise the below can cause unexpected results
        //since we do not perform Date based comparison, if unsorted visits array list
        //is given, then one visit can be merged with visit which is Date.before() it;
        //the correct way is to merge current visit with the next visit which Date.after() it
        //Collections.sort(visits); //visits are sorted before mergeVisits() call, therefore we do not call this

        ArrayList<Visit> mergedVisits = new ArrayList<Visit>();
        //System.out.println("Number of merged visits: " + mergedVisits.size());

        //add the original visit at index 0 as the starting point for merged visits
        mergedVisits.add(Visit.newInstance(visits.get(0)));

        //for loop to achieve merge operation
        for(int index = 1; index < visits.size(); index ++)
        {
            //nextVisit and lastMergedVisit
            Visit nextVisit = visits.get(index);
            Visit lastMergedVisit = mergedVisits.get(mergedVisits.size() - 1);

            //get their day seconds and residence seconds
            long nextVisitDaySeconds = nextVisit.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
            long lastMergedVisitDaySeconds = lastMergedVisit.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
            long lastMergedVisitResidenceSeconds = lastMergedVisit.getTimeSeriesInstant().getResidenceTimeInSeconds();

            //lastMergedVisitDaySeconds + lastMergedVisitResidenceSeconds can surpass 86400
            //then perform mod 86400 to make addition in the interval of [0, 86400]
            long lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400
                    = (lastMergedVisitDaySeconds + lastMergedVisitResidenceSeconds) % 86400;

            //t_i+1 - (t_i + d_i) should non-negative, since the user's visit
            //history can contain visit at 12:00 o'clock midnight and at 1:00 p.m.
            //where 3600 - 86400 is negative.
            //we choose the interval of delta seconds to get rid periodic sampling of GPS data
            //we have added the case when the distance is not more than 86400 seconds or 1 day
            //assume that nextVisitDaySeconds is 3660 in March 10 and lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 is 3600 in March 5, assume residence time is 0 seconds
            //previously[OLD] below statement was satisfied but residence seconds would be 60 seconds [missing about 5 days between March 10 and March 5]
            if((nextVisitDaySeconds - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400
                    <= deltaThresholdInSeconds)
                    && (nextVisitDaySeconds - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400) >= 0)
            {
                //one time series instant s contains arrival time t and residence d; s = (t, d).
                //create new visit with arrival time of t_i and and residence time with t_i+1 - t_i + d_i+1
                //namely s_i = (t_i, t_i+1 - t_i + d_i+1), since t_i+1 - t_i contains previous merged visit's d_i

                // ===== NEW =====
                //Date.getTime() returns the number of milliseconds since January 1, 1970, 00:00:00 GMT
                //represented by calling Date object
                //Date lastMergedVisitArrivalTimePlusResidenceTime
                //= new Date(lastMergedVisit.getTimeSeriesInstant().getArrivalTime().getTime()
                //+ lastMergedVisitResidenceSeconds * 1000); // * 1000 to make milliseconds

                //not used lastMergedVisitArrivalTimePlusResidenceTime,
                //because nextVisitDaySeconds + 86400 - lastMergedVisitDaySeconds + (noOfDaysBetween - 1) * 86400
                //will include residence seconds of lastMergedVisitArrivalTimePlusResidenceTime
                //in the final residence seconds
                long noOfDaysBetween = Utils.noOfDaysBetween(nextVisit.getTimeSeriesInstant().getArrivalTime(),
                        lastMergedVisit.getTimeSeriesInstant().getArrivalTime());
                if(noOfDaysBetween >= 1)
                {
                    //if noOfDaysBetween is more than 1, then the interval between them
                    //is bigger than deltaThresholdInSeconds above, therefore we will not perform
                    //merge operation
                    //update the residence of the last merged visit
                    lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                            nextVisitDaySeconds
                                    + 86400
                                    - lastMergedVisitDaySeconds // 86400 - lastMergedVisitDaySeconds contains prev. res. seconds
                                    + (noOfDaysBetween - 1) * 86400);
                } // if
                else
                {
                    //We have updated mergeWith() method for the following rare case:
                    //Assume that nextVisit arrival time is 70, lastVisit arrival time is 86330 and residence seconds is 100
                    //then lastVisitArrivalTimePlusResidenceTime is 86430 mod 86400 = 30
                    //where 70 > 30.
                    //now merge with helper method in Visit class
                    Visit mergedVisit = lastMergedVisit.mergeWith(nextVisit);
                    //update the last element of the array list
                    mergedVisits.set(mergedVisits.size() - 1, mergedVisit);
                } //else
                // ===== NEW =====



                // ===== OLD =====
                /*
                //now merge with helper method in Visit class
                Visit mergedVisit = lastMergedVisit.mergeWith(nextVisit);
                //update the last element of the array list
                mergedVisits.set(mergedVisits.size() - 1, mergedVisit);
                */
                // ===== OLD =====
            } // if
            //we have added the case  when the distance is not more than 86400 seconds or 1 day
            //assume that nextVisitDaySeconds is 3600 in March 10 and lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 is 3800 in March 5, assume residence time is 0 seconds
            //previously[OLD] below statement is satisfied but residence seconds will be 86200 seconds [missing about 5 days between March 10 and March 5]
            else if(nextVisitDaySeconds - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 < 0)
            {
                // ===== NEW =====
                //not used lastMergedVisitArrivalTimePlusResidenceTime,
                //because nextVisitDaySeconds + 86400 - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 + (noOfDaysBetween - 1) * 86400
                //will be used to calculate possibleResidenceSeconds and it will be added lastMergedVisitResidenceSeconds
                //as a new residence seconds
                long noOfDaysBetween = Utils.noOfDaysBetween(nextVisit.getTimeSeriesInstant().getArrivalTime(),
                        lastMergedVisit.getTimeSeriesInstant().getArrivalTime());

                //possible residence seconds between nextVisitArrivalTime
                //and lastMergedVisitArrivalTimePlusResidenceTime
                long possibleResidenceSeconds = 0; //by default

                if(noOfDaysBetween >= 1)
                {
                    possibleResidenceSeconds = nextVisitDaySeconds
                            + 86400 - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400
                            + (noOfDaysBetween - 1) * 86400;

                    if(possibleResidenceSeconds <= deltaThresholdInSeconds)
                    {
                        //update the residence of the last merged visit
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                lastMergedVisitResidenceSeconds + possibleResidenceSeconds);
                    } // if
                    else
                    {
                        //again update the residence of the last merged visit
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                lastMergedVisitResidenceSeconds + possibleResidenceSeconds);

                        //and create a new merged visit
                        Visit newVisit = new Visit(nextVisit.getUserName(), nextVisit.getLocation(),
                                nextVisit.getTimeSeriesInstant());
                        //add it to the array list
                        mergedVisits.add(newVisit);
                    } // else
                } // if
                else
                {   //if visits are on the same day, then
                    //ACCORDING TO CHRONOLOGICAL ORDER OR DATE ORDER THIS CONDITION CANNOT HAPPEN;
                    //namely nextVisitDaySeconds - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400
                    //is non-negative, because of Date order.

                    //they are definitely in different days, such as:
                    //Date:     Arr:    Res:
                    //March 8   2100    600
                    //March 9   2000    0
                    //-----------------------
                    //then noOfDaysBetween is 0, therefore:
                    possibleResidenceSeconds = nextVisitDaySeconds
                            + 86400 - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400;

                    //check against threshold
                    if(possibleResidenceSeconds <= deltaThresholdInSeconds)
                    {
                        //update the residence of the last merged visit
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                lastMergedVisitResidenceSeconds + possibleResidenceSeconds);
                    } // if
                    else
                    {
                        //again update the residence of the last merged visit
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                lastMergedVisitResidenceSeconds + possibleResidenceSeconds);

                        //and create a new merged visit
                        Visit newVisit = new Visit(nextVisit.getUserName(), nextVisit.getLocation(),
                                nextVisit.getTimeSeriesInstant());
                        //add it to the array list
                        mergedVisits.add(newVisit);
                    } // else
                } // else
                // ===== NEW =====


                // ===== OLD =====
                /*
                //this is the case, nextVisitDaySeconds is on the next day
                //and lastMergedVisitDaySeconds is on the previous day
                //lastMergedVisitDaySeconds + lastMergedVisitResidenceSeconds will guarantee
                //the sensor's last arrival time reading in previous day
                //for example let's say nextVisitDaySeconds is 3000,
                //but lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 is 3660
                //it means that last visit happened in previous day
                //and nextVisit happened in the next day
                //therefore perform 86400 - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400
                //and add nextVisitDaySeconds to the result
                long possibleResidenceSeconds = nextVisitDaySeconds
                                                + 86400 - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400;
                if(possibleResidenceSeconds <= deltaThresholdInSeconds)
                {
                    //update the residence of the last merged visit
                    lastMergedVisit
                            .getTimeSeriesInstant()
                            .setResidenceTimeInSeconds(lastMergedVisitResidenceSeconds + possibleResidenceSeconds);
                } // if
                else
                {
                    //again update the residence of the last merged visit
                    lastMergedVisit
                            .getTimeSeriesInstant()
                            .setResidenceTimeInSeconds(lastMergedVisitResidenceSeconds + possibleResidenceSeconds);

                    //and create a new merged visit
                    Visit newVisit = new Visit(nextVisit.getLocation(), nextVisit.getTimeSeriesInstant());
                    //add it to the array list
                    mergedVisits.add(newVisit);
                } // else
                */
                // ===== OLD =====

            } // else if
            else        //nextVisitDaySeconds - lastMergedVisitArrivalTimePlusResidenceTimeInDaySecondsMod86400 > deltaThresholdInSeconds
            {
                // ===== NEW =====
                //we have added the case  when the distance is not more than 86400 seconds or 1 day
                //assume you perform following merge
                //there is also noOFDaysBetween issue
                //Date:     Arr:    Res:
                //May 18    100     100
                //May 19    1915    0
                //----------------------
                //May 18    100     1815(residence seconds miss more than day, or more than 86400 seconds)
                //                      (correct residence time should be 86400 - 100 + 1915 = 88215 seconds)
                Date nextVisitDate = nextVisit.getTimeSeriesInstant().getArrivalTime();
                Date lastMergedVisitDate = lastMergedVisit.getTimeSeriesInstant().getArrivalTime();
                long noOfDaysBetween = Utils.noOfDaysBetween(nextVisitDate, lastMergedVisitDate);

                if(noOfDaysBetween >= 1)
                {
                    //update the last merged visit
                    lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                            nextVisitDaySeconds
                                    + 86400
                                    - lastMergedVisitDaySeconds // 86400 - lastMergedVisitDaySeconds contains prev. res. seconds
                                    + (noOfDaysBetween - 1) * 86400);
                } // if
                else        //no of days between is 0
                {
                    //if visits are not on the same day
                    if(!Utils.onTheSameDay(nextVisitDate, lastMergedVisitDate))
                    {
                        //update the last merged visit
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                nextVisitDaySeconds
                                        + 86400
                                        - lastMergedVisitDaySeconds); // contains prev. res. seconds
                    } // if
                    else        //visits are on the same day
                    {
                        lastMergedVisit.getTimeSeriesInstant().setResidenceTimeInSeconds(
                                nextVisitDaySeconds
                                        - lastMergedVisitDaySeconds); // contains prev. res. seconds
                        //+ lastMergedVisitResidenceSeconds);
                    } // else
                } // else

                //in any case we should create new visit from the original one at index "index"
                Visit newVisit = new Visit(nextVisit.getUserName(), nextVisit.getLocation(),
                        nextVisit.getTimeSeriesInstant());
                //add it to the array list
                mergedVisits.add(newVisit);
                // ===== NEW =====


                // ===== OLD =====
                /*
                //compared visits are not in the interval of delta seconds
                //first update the old visit from merged visits array list
                //the only thing that will be changed is residence time
                //new residence time d_i will be t_i+1 - t_i, since t_i+1 - t_i contains previous ly merged visit's d_i
                // ====== nextVisitDaySeconds - lastMergedVisitDaySeconds will contain intermediately ======
                // ====== calculated residence seconds plus real difference in seconds between ======
                // ====== nextVisitDaySeconds and its previous counterpart from the original unmerged visits ======
                lastMergedVisit
                        .getTimeSeriesInstant()
                        .setResidenceTimeInSeconds(nextVisitDaySeconds
                                                    - lastMergedVisitDaySeconds);
                                                    //+ lastMergedVisitResidenceSeconds);

                //then, create a new one from the original visit at index "index"
                Visit newVisit = new Visit(nextVisit.getLocation(), nextVisit.getTimeSeriesInstant());
                //add it to the array list
                mergedVisits.add(newVisit);
                */
                // ===== OLD =====
            } // else

        } // for

        return mergedVisits;
    } // mergeVisits


    /*
    //overloaded method which sorts merged visits with custom comparator
    //and returns sorted merged visits
    public static ArrayList<Visit> mergeVisits(ArrayList<Visit> visits, int deltaThresholdInSeconds,
                                               Comparator<Visit> customComparator)
    {
        //customComparator can be the following:
//        new Comparator<Visit>()
//        {
//            @Override
//            public int compare(Visit v1, Visit v2)
//            {
//                long v1ArrivalTimeInDaySeconds = v1.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
//                long v2ArrivalTimeInDaySeconds = v2.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
//
//                //return the result
//                return Long.compare(v1ArrivalTimeInDaySeconds, v2ArrivalTimeInDaySeconds);
//            } // compare
//        };


        //ORIGINAL MERGE OPERATION SHOULD BE PERFORMED IN NATURAL ORDERING (for visits it is Date-based ordering)
        ArrayList<Visit> mergedVisits = mergeVisits(visits, deltaThresholdInSeconds);

        //Natural ordering the visits is Date-based, with custom comparator
        //we can make visits to be ordering using its time series instant => by arrival time in day seconds
        //if comparator is null need to sort anything
        if(customComparator != null)
            //List.sort() documentation: If the specified comparator is null then all elements in this list must implement
            //the Comparable interface and the elements' natural ordering should be used.
            mergedVisits.sort(customComparator);

        return mergedVisits;
    } // mergeVisits
    */

    //helper method to sort the given xml file
    //by "id", where parent node will be <cab>
    public static String sortXmlFileInPath(String xmlFilePath, Dataset dataset) //String rootTagName,
                                           //String userTagName, String userAttributeNameToSortBy)
    {
        String rootTagName = dataset.rootTagName();
        String userTagName = dataset.userTagName();
        String userAttributeNameToSortBy = dataset.userAttributeName();

        //create a document from _cabs.xml file
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;

        //now read the original file
        File file = new File(xmlFilePath);
        File resultXMLFile = new File(xmlFilePath.replace(".xml", "")
                + "_sorted.xml");
        BufferedReader reader = null;
        //PrintWriter writer = null;
        String line = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            resultXMLFile.createNewFile();
            //writer = new PrintWriter(resultXMLFile);


            //built all-in-one string from file
            StringBuilder stringBuilder = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            } // while


            //build xml document
            Document doc = null;
            try {
                builder = factory.newDocumentBuilder();
                doc = builder.parse(new InputSource(new StringReader(stringBuilder.toString())));

                //one xml node looks like the following:
                //<cab id="enyenewl" updates="2414"/>
                //Node[] resultNodes = sortNodes(doc.getElementsByTagName("cab"), "id",
                //true, String.class);
                Node[] resultNodes = sortNodes(doc.getElementsByTagName(userTagName), userAttributeNameToSortBy,
                        true, String.class);

                Document xmlDoc = new DocumentImpl();
                //Element root = xmlDoc.createElement("cabs");
                Element root = xmlDoc.createElement(rootTagName);

                for (Node node : resultNodes) {
                    //System.out.println(((Element)node).getAttribute("id"));

                    //node existing in other document can be appended directly
                    //to the other document
                    //trick is just importing a Node to other Document, instead of directly appending
                    //xmlDoc.importNode imports node from doc defined above
                    Node importedNode = xmlDoc.importNode(node, true);
                    root.appendChild(importedNode);
                } // for

                //add root to main xmlDoc
                xmlDoc.appendChild(root);

                //generate new sorted xml file by id attribute
                try {
                    Source source = new DOMSource(xmlDoc);
                    StreamResult result = new StreamResult(new OutputStreamWriter(
                            new FileOutputStream(resultXMLFile), "UTF-8"));
                    Transformer xmlTransformer = TransformerFactory.newInstance().newTransformer();
                    xmlTransformer.transform(source, result);
                } catch (Exception e) {
                    System.err.println("Error creating new xml file: " + e.toString());
                    e.printStackTrace();
                } // catch

            } // try
            catch (Exception e) {
                System.err.println(e.toString());
                e.printStackTrace();
            } // catch

        } // try
        catch (Exception e) {
            System.err.println("Error while reading a file + " + "\"" + file.getPath() + "\":" + e.getMessage());
            e.printStackTrace();
        } // catch
        finally {
            try {
                if (reader != null)
                    reader.close();


            } // try
            catch (Exception e) {
                e.printStackTrace();
            } // catch

            //if (writer != null)
            //writer.close();

        } // finally


        //return the path of the sorted xml file
        return resultXMLFile.getPath();
    } //sortXmlFileInPath

    /**
     * Method sorts any NodeList by provided attribute.
     * @param nl NodeList to sort
     * @param attributeName attribute name to use
     * @param asc true - ascending, false - descending
     * @param B class must implement Comparable and have Constructor(String) - e.g. Integer.class , BigDecimal.class etc
     * @return Array of Nodes in required order
     */
    public static Node[] sortNodes(NodeList nl, String attributeName, boolean asc, Class<? extends Comparable> B)
    {
        class NodeComparator<T> implements Comparator<T>
        {
            @Override
            public int compare(T a, T b)
            {
                int ret;
                Comparable bda = null, bdb = null;
                try{
                    Constructor bc = B.getDeclaredConstructor(String.class);
                    bda = (Comparable)bc.newInstance(((Element)a).getAttribute(attributeName));
                    bdb = (Comparable)bc.newInstance(((Element)b).getAttribute(attributeName));
                }
                catch(Exception e)
                {
                    return 0; // yes, ugly, i know :)
                }
                ret = bda.compareTo(bdb);
                return asc ? ret : -ret;
            }
        }

        List<Node> x = new ArrayList<>();
        for(int i = 0; i < nl.getLength(); i++)
        {
            x.add(nl.item(i));
        }
        Node[] ret = new Node[x.size()];
        ret = x.toArray(ret);
        Arrays.sort(ret, new NodeComparator<Node>());
        return ret;
    } // sortNodes


    //helper method to find a specific file from a folder
    //returns null if file is not found
    public static File findFileByName(final File folder, String fileNameToSearch)
    {
        File foundFile = null;

        for (final File fileEntry : folder.listFiles())
        {
            //Log10.txt file can be returned when searched for Log1
            //this case will happen when Log1.txt file does not exist
            //in the directory; if it exists first found file will be Log1.txt;
            //String.equals(fileNameToSearch)) can be used for exact match;
            //but in this case, file extension should be known beforehand
            //for which global xml does not give any info
            if(fileEntry.getName().contains(fileNameToSearch))
            {
                foundFile = fileEntry;
                break;
            } // if
        } // for

        //return the found file
        return foundFile;
    } // findFileByName


    //default private no-arg constructor to make the class fully static
    private Preprocessing() {}

} // class Preprocessing
