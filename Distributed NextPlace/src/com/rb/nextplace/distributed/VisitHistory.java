package com.rb.nextplace.distributed;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

/**
 * Created by Rufat Babayev on 2/6/2018.
 * VisitHistory will contain only visits of the same user to the same location
 */
public class VisitHistory implements Serializable, Comparable<VisitHistory>
{
    //generated serial version UID
    private static final long serialVersionUID = -8298416078581166472L;

    //we assume that created visit history will contain
    //visits sorted in chronological order, otherwise e.g. TreeSet can be used
    private ArrayList<Visit> visits;
    private ArrayList<TimeSeriesInstant> timeSeries; // timeSeries are obtained from each visit in add() method
    private String userName;
    private Location location;
    private long totalResidenceSeconds;

    //unique identifier for any visit history
    //UUID has a 1 in a billion collision probability
    private UUID vhID;


    //constructor
    /*
    public VisitHistory(Visit v)
    {
        this.visits = new ArrayList<Visit>();
        this.location = v.getLocation();
        visits.add(v);
    } // VisitHistory
    */

    //private constructor to create an empty visit history with userName and location
    private VisitHistory(String userName, Location location)
    {
        //set user name and location
        this.userName = userName;
        this.location = location;


        //assign cryptographically strong unique id
        this.vhID = UUID.randomUUID();


        visits = new ArrayList<Visit>(); // empty list, default
        timeSeries = new ArrayList<TimeSeriesInstant>(); // empty list, by default
        totalResidenceSeconds = 0; // initially 0
    } // VisitHistory

    public VisitHistory()
    {
        //empty string username, by default;
        //default location is location("", 0.0, 0.0) which is in the Gulf of Guinea in the Atlantic Ocean,
        //about 380 miles (611 kilometers) south of Ghana and 670 miles (1078 km) west of Gabon;
        //so it is in the middle of the ocean, therefore no significance;
        //its has a name which is an empty string
        this("", new Location("",0.0,0.0));
    } // VisitHistory

    public void add(Visit v)
    {
        if(this.userName.equals("")
                && this.location.getName().equals("")
                && this.location.getLatitude() == 0.0
                && this.location.getLongitude() == 0.0 && visits.isEmpty())
        {
            this.userName = v.getUserName();
            this.location = v.getLocation();

            //we do not want the original visit to be updated
            //create a new visit instance
            //Visit visitInstance = Visit.newInstance(v); //DO NOT CREATE NEW VISIT
            visits.add(v); //(visitInstance);

            //update timeSeries as well
            //where visitInstance.getTimeSeriesInstant() is the same as v.getTimeSeriesInstant()
            timeSeries.add(v.getTimeSeriesInstant()); //(visitInstance.getTimeSeriesInstant());

            //update the totalResidenceSeconds as well
            totalResidenceSeconds += v.getTimeSeriesInstant().getResidenceTimeInSeconds(); //visitInstance.getTimeSeriesInstant().getResidenceTimeInSeconds();


            //update instantIndex of the timerSeriesInstant of the visitInstance
            //DO NOT UPDATE ORIGINAL VISIT
            //visitInstance.getTimeSeriesInstant().setInstantIndex(visits.size() - 1);
        } // if
        else if(this.userName.equals(v.getUserName()))
        {
            //user names are equal, if the locations is also the same add Visit v to this visit history
            if( this.location.equals(v.getLocation()) )
            {
                //we do not want the original visit to be updated
                //create a new visit instance
                //Visit visitInstance = Visit.newInstance(v);  //DO NOT CREATE NEW VISIT
                visits.add(v); //(visitInstance);

                //update timeSeries as well
                //where visitInstance.getTimeSeriesInstant() is the same as v.getTimeSeriesInstant()
                timeSeries.add(v.getTimeSeriesInstant()); //(visitInstance.getTimeSeriesInstant());

                //update the totalResidenceSeconds as well
                totalResidenceSeconds += v.getTimeSeriesInstant().getResidenceTimeInSeconds(); //visitInstance.getTimeSeriesInstant().getResidenceTimeInSeconds();

                //update instantIndex of the timerSeriesInstant of the visitInstance
                //DO NOT UPDATE ORIGINAL VISIT
                //visitInstance.getTimeSeriesInstant().setInstantIndex(visits.size() - 1);
            } // if
            //if the locations are different report it
            else
            {
                System.err.println("Visit is not added, because it is not performed to the same location -> "
                        + this.location + " <=> " + v.getLocation() + "\n");
                        //+ "\nas other visits in the visit history\n");
            } // else

        } // if
        else
        {
            System.err.println("Visit is not added, because it does not belong to the same user -> "
                    + this.userName + " <=> " + v.getUserName() + "\n");
                    //+ "\nas other visits in the visit history\n");
        } // else
    } // add

    public ArrayList<Visit> getVisits() {
        return visits;
    } // getVisits

    //helper to get the time series of Visit history
    public ArrayList<TimeSeriesInstant> getTimeSeries()
    {
        //ArrayList<TimeSeriesInstant> timeSeries = new ArrayList<TimeSeriesInstant>();
        //for(Visit v : visits)
        //{
        //    //we assume visits are in chronological order
        //    timeSeries.add(v.getTimeSeriesInstant());
        //} // for

        //list is populated in add method, so no need for above code snippet which is inefficient
        return timeSeries;
    } // getTimeSeries

    public String getUserName() { return userName; } // getUserName

    public Location getLocation() {
        return location;
    } // getLocation

    public int getFrequency()
    {
        return visits.size();
    } // frequency


    //helper method to check whether this visit history is empty
    //visit history is empty if its visits array list is empty
    public boolean isEmpty()
    {
        return visits.isEmpty();
    } // isEmpty

    //helper method to get the total residence seconds of this visit history
    //since the visit history contains visits performed to the same location
    //total residence seconds will the sum of residence seconds consumed for each visit in the same location
    public long getTotalResidenceSeconds()
    {
        return totalResidenceSeconds;
    } // getTotalResidenceSeconds

    //maxPossibleK method which will be calculated based on given npConf argument
    //it basically returns maximum number of predictable visits that
    //can be predicted from this visit history based on npConf and its frequency
    public int maxPossibleK(NextPlaceConfiguration npConf)
    {
        //getFrequency() - (embeddingParameterM - 1) * delayParameterV is the size of embedding space
        //there last embedding vector's index will be size of embedding space - 1;
        //maxK is always equal to size of embedding space - 1
        return (getFrequency() - 1) - (npConf.getEmbeddingParameterM() - 1) * npConf.getDelayParameterV();
    } // maxPossibleK


    //method to get the specific visit from visit history by time series instant
    public Visit getVisit(TimeSeriesInstant instant)
    {
        //create a visit with this visit history's userName, location and given time series instant
        //and perform binary search; since visits is sorted chronologically, binary search will work and return
        //the searched visit in O(log(n)) time
        int resultIndex = Collections.binarySearch(visits, new Visit(userName, location, instant));
        if (resultIndex >= 0)
            return visits.get(resultIndex);
        else
            return null; // binarySearch returns negative number if key is not found
    } // getVisit



    //other divide method for given k elements for test split
    public Tuple2<VisitHistory, VisitHistory> divide(NextPlaceConfiguration npConf, int kElementForTestSplit)
    {
        if(isTwoDividable(npConf, kElementForTestSplit))
        {
            //obtain length of this visit history
            int frequency = getFrequency();

            //calculate the length of first and second part
            int lengthOfSecondPart = kElementForTestSplit;
            if(lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
            int lengthOfFirstPart = frequency - lengthOfSecondPart;

            //if by integer division accidentally first part and second part have same size, make second part one smaller
            //and first part one bigger
            if(lengthOfFirstPart == lengthOfSecondPart)
            {
                lengthOfSecondPart -= 1;
                lengthOfFirstPart += 1;
            } // if



            //create first part
            VisitHistory firstPart = new VisitHistory();
            //add all visits up to lengthOfFirstPart
            for(int index = 0; index < lengthOfFirstPart; index ++)
            {
                firstPart.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
            } // for


            //first index of second part will start from lengthOfFirstPart
            VisitHistory secondPart = new VisitHistory();
            for(int index = lengthOfFirstPart; index < frequency; index ++)
            {
                secondPart.add(visits.get(index));  // new instance of visit is created in add method from the provided visit
            } // for

            return new Tuple2<>(firstPart, secondPart);
        } // if
        else
            throw new RuntimeException("Visit history is not dividable");
    } // divide


    //helper method to divide visit history into two parts, namely train split and test split
    //it will return Tuple2 of first part and second part
    public Tuple2<VisitHistory, VisitHistory> divide(NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction)
    {
        //if the given configuration is two dividable
        if(isTwoDividable(npConf, trainSplitFraction, testSplitFraction))
        {
            //obtain length of this visit history
            int frequency = getFrequency();

            //calculate the length of first and second part
            int lengthOfSecondPart = (int) (frequency * testSplitFraction);
            if(lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
            int lengthOfFirstPart = frequency - lengthOfSecondPart;

            //if by integer division accidentally first part and second part have same size, make second part one smaller
            //and first part one bigger
            if(lengthOfFirstPart == lengthOfSecondPart)
            {
                lengthOfSecondPart -= 1;
                lengthOfFirstPart += 1;
            } // if



            //create first part
            VisitHistory firstPart = new VisitHistory();
            //add all visits up to lengthOfFirstPart
            for(int index = 0; index < lengthOfFirstPart; index ++)
            {
                firstPart.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
            } // for


            //first index of second part will start from lengthOfFirstPart
            VisitHistory secondPart = new VisitHistory();
            for(int index = lengthOfFirstPart; index < frequency; index ++)
            {
                secondPart.add(visits.get(index));  // new instance of visit is created in add method from the provided visit
            } // for

            return new Tuple2<>(firstPart, secondPart);
        } // if
        else // else return two empty visit histories with the same user name and location
            //return new Tuple2<>(new VisitHistory(this.userName, this.location), new VisitHistory(this.userName, this.location));
            throw new RuntimeException("Visit history is not dividable");
    } // divide


    //separate method for getting train split
    public VisitHistory getTrainSplit(NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction)
    {
        //if the given configuration is two dividable
        if(isTwoDividable(npConf, trainSplitFraction, testSplitFraction))
        {
            //obtain length of this visit history
            int frequency = getFrequency();

            //calculate the length of first and second part
            int lengthOfSecondPart = (int) (frequency * testSplitFraction);
            if (lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
            int lengthOfFirstPart = frequency - lengthOfSecondPart;

            //if by integer division accidentally first part and second part have same size, make second part one smaller
            //and first part one bigger
            if(lengthOfFirstPart == lengthOfSecondPart)
            {
                lengthOfSecondPart -= 1;
                lengthOfFirstPart += 1;
            } // if


            //create first part
            VisitHistory firstPart = new VisitHistory();
            //add all visits up to lengthOfFirstPart
            for (int index = 0; index < lengthOfFirstPart; index++) {
                firstPart.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
            } // for

            return firstPart;
        } // if
        else //return empty visit history with the same user name and location
            //return new VisitHistory(this.userName, this.location);
            throw new RuntimeException("Visit history is not dividable");
    } // getTrainSplit



    //separate method for getting test split
    public VisitHistory getTestSplit(NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction)
    {
        //if the given configuration is two dividable
        if(isTwoDividable(npConf, trainSplitFraction, testSplitFraction))
        {
            //obtain length of this visit history
            int frequency = getFrequency();

            //calculate the length of first and second part
            int lengthOfSecondPart = (int) (frequency * testSplitFraction);
            if (lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
            int lengthOfFirstPart = frequency - lengthOfSecondPart;

            //if by integer division accidentally first part and second part have same size, make second part one smaller
            //and first part one bigger
            if(lengthOfFirstPart == lengthOfSecondPart)
            {
                lengthOfSecondPart -= 1;
                lengthOfFirstPart += 1;
            } // if


            //first index of second part will start from lengthOfFirstPart
            VisitHistory secondPart = new VisitHistory();
            for(int index = lengthOfFirstPart; index < frequency; index ++)
            {
                secondPart.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
            } // for

            return secondPart;
        } // if
        else //return empty visit history with the same user name and location
            //return new VisitHistory(this.userName, this.location);
            throw new RuntimeException("Visit history is not dividable");
    } // getTestSplit



    public boolean isTwoDividable(NextPlaceConfiguration npConf, double trainSplitFraction, double testSplitFraction)
    {
        if(Double.compare(trainSplitFraction + testSplitFraction, 1.0) != 0)
            throw new RuntimeException("Train and test split fractions must add up to 1");

        if(testSplitFraction > trainSplitFraction)
            throw new RuntimeException("Test split fraction cannot be bigger than train split fraction");


        //obtain length of this visit history
        int frequency = getFrequency();

        //For division into two parts:
        //1) frequency should be at least 3
        //2) visit history should be predictable which means maxK should be at least one
        //3) first part should be predictable
        if(frequency >= 3 && maxPossibleK(npConf) >= 1)
        {
            //calculate the length of first and second part
            int lengthOfSecondPart = (int) (frequency * testSplitFraction);
            if(lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
            int lengthOfFirstPart = frequency - lengthOfSecondPart;

            //if by integer division accidentally first part and second part have same size, make second part one smaller
            //and first part one bigger
            if(lengthOfFirstPart == lengthOfSecondPart)
            {
                lengthOfSecondPart -= 1;
                lengthOfFirstPart += 1;
            } // if


            //now if the first part is predictable (maxK >= 1), then visit history can be divided into two parts
            return Utils.maxValueOfFutureStepK(npConf, lengthOfFirstPart) >= 1;
        } // if
        else
            return false;
    } // isTwoDividable


    public boolean isTwoDividable(NextPlaceConfiguration npConf, int kElementsForTestSplit)
    {
        assert kElementsForTestSplit >= 1;


        //obtain length of this visit history
        int frequency = getFrequency();

        //For division into two parts:
        //1) frequency should be at least 3
        //2) first part should be predictable
        //check will ensure => f >= 2k + 1 => which will ensure that first part f - k >= k + 1
        //such that the first part will be predictable
        return frequency >= 3
                && Utils.maxValueOfFutureStepK(npConf, frequency - kElementsForTestSplit) >= kElementsForTestSplit;
    } // isTwoDividable


    //method to return whether the visit history is twoDividable
    public boolean isHalfDividable(NextPlaceConfiguration npConf)
    {
        //both createVisitHistories() methods in Utils class creates predictable visit histories
        //in this case, the following call is the sanity check whether some random visit history is predictable
        //if not then it is indeed not two dividable
        int frequency = getFrequency();


        //For two division:
        //1) frequency should be at least 3
        //2) visit history should be predictable which means maxK should be at least one
        //3) first half should be predictable
        if(frequency >= 3 && maxPossibleK(npConf) >= 1)
        {
            //regardless of frequency is an odd or even number, after division length (frequency) of the first half
            //will be calculated using the same way
            int lengthOfTheFirstHalf = (frequency / 2) + 1;

            //now if the first half is predictable (maxK >= 1), then visit history is half dividable
            return Utils.maxValueOfFutureStepK(npConf, lengthOfTheFirstHalf) >= 1;
        } // if
        else
            return false;
    } // isHalfDividable




//    //helper method to get the first half of this visit history as a new visit history
//    public VisitHistory getFirstHalf(NextPlaceConfiguration npConf)
//    {
//        if(isHalfDividable(npConf))
//        {
//            VisitHistory newVisitHistory = new VisitHistory();
//            //regardless of frequency is an odd or even number, length of the first half is always the same
//            int lengthOfTheFirstHalf = (getFrequency() / 2) + 1;
//
//            //now just add visits up to the lengthOfTheFirstHalf to the new visit history
//            for(int index = 0; index < lengthOfTheFirstHalf; index ++)
//            {
//                newVisitHistory.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
//            } // for
//
//
//            //return the populated visit history
//            return newVisitHistory;
//        } // if
//        else
//        {
//            //return an empty visit history with the same user name and location
//            return new VisitHistory(this.userName, this.location);
//        } // else
//    } // getFirstHalf
//
//
//    //helper method to get the second half of this visit history as a new visit history
//    public VisitHistory getSecondHalf(NextPlaceConfiguration npConf)
//    {
//        if(isHalfDividable(npConf))
//        {
//            VisitHistory newVisitHistory = new VisitHistory();
//            int frequency = getFrequency(); // length of this visit history
//
//            //regardless of frequency is odd or even number, starting index for second is always the same
//            int startIndex = (frequency / 2) + 1;
//
//            //now just add all visits starting from startIndex to new visit history
//            for(int index = startIndex; index < frequency; index ++)
//            {
//                newVisitHistory.add(visits.get(index)); // new instance of visit is created in add method from the provided visit
//            } // for
//
//
//            //return the populated visit history
//            return newVisitHistory;
//        } // if
//        else
//        {
//            //return an empty visit history with the same user name and location
//            return new VisitHistory(this.userName, this.location);
//        } // else
//    } // getSecondHalf


    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof  VisitHistory))
        {
            return false;
        } // if
        else if(this == other)
        {
            return true;
        } // else if
        else
        {
            //they are equal if they belong to the same user and location is the same
            return this.userName.equals(((VisitHistory) other).getUserName())
                    &&  this.location.equals(((VisitHistory) other).getLocation()) //;
                    && this.vhID.equals(((VisitHistory) other).getVhID());
        } // else
    } // equals


    //corresponding hashcode for the above equals() method
    //visit histories are the same if they belong to the same user and are performed to the same location
    //visit histories with the same username and location cannot have different timeSeries
    //since during creation of VisitHistory another TimeSeriesInstants are appended to the existing timeSeries;
    //Or even if there exist different timeSeries (TimeSeriesInstants) of the same user performed to the same location, they are appended together
    //during creation of the VisitHistory.
    @Override
    public int hashCode()
    {
        //return the result of the convenience method
        return Objects.hash(userName, location, vhID); //);
    } // hashCode


    //override a method from java.lang.Comparable
    @Override
    public int compareTo(VisitHistory other)
    {
        ////if the username are the same, compare with locations
        ////otherwise compare with alphabetical order (of the user names)
        //if(this.userName.equals(other.getUserName()))
        //    return this.location.compareTo(other.getLocation());
        //else
        //    return this.userName.compareTo(other.getUserName());



        //compare against username -> location -> vhID
        if(this.userName.equals(other.getUserName()))
        {
            if(this.location.equals(other.getLocation()))
            {
                return this.vhID.compareTo(other.getVhID());
            } // if
            else
                return this.location.compareTo(other.getLocation());
        } // if
        else
            return this.userName.compareTo(other.getUserName());
    } // compareTo


    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for(Visit v : visits)
            stringBuilder.append(v).append("\n");

        return stringBuilder.toString();
    } // toString

    //toString with date method
    public String toStringWithDate()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for(Visit v : visits)
            stringBuilder.append(v.toStringWithDate()).append("\n");

        return stringBuilder.toString();
    } // toStringWithDate

    //clear method to clear visits and time series
    public void clear()
    {
        visits.clear();
        timeSeries.clear();

        visits = new ArrayList<Visit>(); // empty list, default
        timeSeries = new ArrayList<TimeSeriesInstant>(); // empty list, by default
        totalResidenceSeconds = 0; // initially 0
    } // clear



    //getter method for id of this visit history
    public UUID getVhID() {
        return vhID;
    } // getVhID



//    //helper method to calculate the standard deviation of the visit history
//    public double variance()
//    {
//        return Utils.populationVarianceOfTimeSeries(this.timeSeries);
//    } // variance
//
//
//    //helper method to calculate standard deviation of the visit history
//    public double std()
//    {
//        return Math.sqrt(variance());
//    } // std
} // class VisitHistory
