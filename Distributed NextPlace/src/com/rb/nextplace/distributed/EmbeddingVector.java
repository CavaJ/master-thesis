package com.rb.nextplace.distributed;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Rufat Babayev on 2/6/2018.
 */
public class EmbeddingVector implements Serializable
{
    //generated serial version UID
    private static final long serialVersionUID = 7790631386718107509L;

    //Embedding vector refers to betas and it will contain time series instants e.g. s_0, s_1, s_2 and so on
    //timeSeries array list should be already sorted
    private ArrayList<TimeSeriesInstant> timeSeries;

    //constructor
    public EmbeddingVector()
    {
        timeSeries = new ArrayList<>();
    } // EmbeddingVector

    public void add(TimeSeriesInstant instant)
    {
        timeSeries.add(instant);
    } // add

    //method to calculate distance between two embedding vectors
    //embedding vectors have to have the same size
    public double distanceTo(EmbeddingVector other, NextPlaceConfiguration npConf)
    {
        if(timeSeries.size() != other.getTimeSeries().size())
            throw new IllegalArgumentException("Embedding vectors must have the same size");
        if(timeSeries.isEmpty() || other.getTimeSeries().isEmpty())
            throw new IllegalArgumentException("Embedding vectors cannot be empty, they should have at least one time series instant");


        //switch distance metrics for calculating distances between embedding vectors
        switch(npConf.getDistanceMetric())
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

                for (int index = 0; index < timeSeries.size(); index++) {
                    cumulativeDistance += timeSeries.get(index).distanceInSecondsTo(other.getTimeSeries().get(index));
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
                for (int index = 0; index < timeSeries.size(); index++)
                {
                    double distance = timeSeries.get(index).distanceInSecondsTo(other.getTimeSeries().get(index));

                    //square the distance and sum up the squared distances
                    cumulativeSquaredDistance += distance * distance;
                } // for

                //return the sqrt of cumulative squared distance
                return Math.sqrt(cumulativeSquaredDistance);
            } // case Euclidean


            case Chebyshev: {
                //Chebyshev distance (or Tchebychev distance), maximum metric, or L∞ metric is a metric defined on
                //a vector space where the distance between two vectors is the greatest of their differences along
                //any coordinate dimension.

                double distanceChebyshev = 0;
                for (int index = 0; index < timeSeries.size(); index++)
                {
                    double distanceIndexIndex = timeSeries.get(index).distanceInSecondsTo(other.getTimeSeries().get(index));
                    if(distanceIndexIndex > distanceChebyshev)
                        distanceChebyshev = distanceIndexIndex;
                } // for

                //return maximum difference between coordinate pairs
                return distanceChebyshev;
            } // case Chebyshev


            case ComparativeManhattan: {

                //if timerSeries.size() is 1 (this happens when embedding parameter m = 1), then
                //thisComparativeDistances.size() and otherComparativeDistances.size() will be 0
                //timeSeries.size() = 1 means that there is only instant there,
                //in this case otherTimeSeries also has one instant, so return the distance between them
                if(timeSeries.size() == 1) // otherTimeSeries.size() is also equal to 1
                    return timeSeries.get(0).distanceInSecondsTo(other.getTimeSeries().get(0));


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
                //ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                //ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                //ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                //for (int index = 0; index < timeSeries.size() - 1; index++)
                //{
                //    //do comparative distance calculation for the first embedding vector
                //    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                //    //add it to the first array list
                //    thisComparativeDistances.add(thisComparativeDistance);
                //
                //    //now do the the same thing for the other embedding vector
                //    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                //    //add it to the second array list
                //    otherComparativeDistances.add(otherComparativeDistance);
                //} // for


                ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                double[] thisComparativeDistances = new double[timeSeries.size() - 1];
                double[] otherComparativeDistances = new double[timeSeries.size() - 1];
                for(int index = 0; index < timeSeries.size() - 1; index ++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                    //add it to the first array
                    thisComparativeDistances[index] = thisComparativeDistance;

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                    //add it to the second array
                    otherComparativeDistances[index] = otherComparativeDistance;
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------

                double cumulativeComparativeDistanceDifference = 0;

                //now find the difference between corresponding components of comparative distances
                //thisComparativeDistances and otherComparativeDistances have the same size
                for (int index = 0; index < thisComparativeDistances.length; /*.size();*/ index ++)
                {
                    //e.g. d_x = d_1_0 - d_8_7
                    double comparativeDistanceDifference
                            = Math.abs(
                                    //thisComparativeDistances.get(index) - otherComparativeDistances.get(index)
                                    thisComparativeDistances[index] - otherComparativeDistances[index]
                                    );

                    //sum them up
                    cumulativeComparativeDistanceDifference += comparativeDistanceDifference;
                } // for

                return cumulativeComparativeDistanceDifference;
            } // case Manhattan


            case ComparativeEuclidean: {

                //if timerSeries.size() is 1 (this happens when embedding parameter m = 1), then
                //thisComparativeDistances.size() and otherComparativeDistances.size() will be 0
                //timeSeries.size() = 1 means that there is only instant there,
                //in this case otherTimeSeries also has one instant, so return the distance between them
                if(timeSeries.size() == 1) // otherTimeSeries.size() is also equal to 1
                    return timeSeries.get(0).distanceInSecondsTo(other.getTimeSeries().get(0));


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
                //ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                //ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                //ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                //for (int index = 0; index < timeSeries.size() - 1; index++)
                //{
                //    //do comparative distance calculation for the first embedding vector
                //    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                //    //add it to the first array list
                //    thisComparativeDistances.add(thisComparativeDistance);
                //
                //    //now do the the same thing for the other embedding vector
                //    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                //    //add it to the second array list
                //    otherComparativeDistances.add(otherComparativeDistance);
                //} // for

                ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                double[] thisComparativeDistances = new double[timeSeries.size() - 1];
                double[] otherComparativeDistances = new double[timeSeries.size() - 1];
                for(int index = 0; index < timeSeries.size() - 1; index ++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                    //add it to the first array
                    thisComparativeDistances[index] = thisComparativeDistance;

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                    //add it to the second array
                    otherComparativeDistances[index] = otherComparativeDistance;
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------

                double cumulativeSquaredComparativeDistanceDifference = 0;

                //now find the difference between corresponding components of comparative distances
                //thisComparativeDistances and otherComparativeDistances have the same size
                for(int index = 0; index < thisComparativeDistances.length; /*.size();*/ index ++)
                {
                    //e.g. d_x = d_1_0 - d_8_7
                    double comparativeDistanceDifference
                            = Math.abs(
                                    //thisComparativeDistances.get(index) - otherComparativeDistances.get(index)
                                    thisComparativeDistances[index] - otherComparativeDistances[index]
                                    );

                    //square the distance and sum the results up
                    cumulativeSquaredComparativeDistanceDifference
                            += comparativeDistanceDifference * comparativeDistanceDifference;
                } // for

                return Math.sqrt(cumulativeSquaredComparativeDistanceDifference);

            } // case ComparativeEuclidean



            case ComparativeChebyshev: {

                //if timerSeries.size() is 1 (this happens when embedding parameter m = 1), then
                //thisComparativeDistances.size() and otherComparativeDistances.size() will be 0
                //timeSeries.size() = 1 means that there is only instant there,
                //in this case otherTimeSeries also has one instant, so return the distance between them
                if(timeSeries.size() == 1) // otherTimeSeries.size() is also equal to 1
                    return timeSeries.get(0).distanceInSecondsTo(other.getTimeSeries().get(0));


                //ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                //ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                //ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                //for (int index = 0; index < timeSeries.size() - 1; index++)
                //{
                //    //do comparative distance calculation for the first embedding vector
                //    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                //    //add it to the first array list
                //    thisComparativeDistances.add(thisComparativeDistance);
                //
                //   //now do the the same thing for the other embedding vector
                //    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                //    //add it to the second array list
                //    otherComparativeDistances.add(otherComparativeDistance);
                //} // for

                ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                double[] thisComparativeDistances = new double[timeSeries.size() - 1];
                double[] otherComparativeDistances = new double[timeSeries.size() - 1];
                for(int index = 0; index < timeSeries.size() - 1; index ++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                    //add it to the first array
                    thisComparativeDistances[index] = thisComparativeDistance;

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                    //add it to the second array
                    otherComparativeDistances[index] = otherComparativeDistance;
                } // for


                double distanceCustomChebyshev = 0;
                //now find the difference between corresponding components of comparative distances
                //thisComparativeDistances and otherComparativeDistances have the same size
                for (int index = 0; index < thisComparativeDistances.length; /*.size();*/ index ++)
                {
                    //e.g. d_x = d_1_0 - d_8_7
                    double comparativeDistanceDifference
                            = Math.abs(
                                    //thisComparativeDistances.get(index) - otherComparativeDistances.get(index)
                                    thisComparativeDistances[index] - otherComparativeDistances[index]
                                    );

                    if(distanceCustomChebyshev > comparativeDistanceDifference)
                        distanceCustomChebyshev = comparativeDistanceDifference;
                } // for

                //return max comparative distance difference
                return distanceCustomChebyshev;
            } // case Chebyshev



            case ComparativeLoopingDistance: {

                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------
                //if timerSeries.size() is 1 (this happens when embedding parameter m = 1), then
                //thisComparativeDistances.size() and otherComparativeDistances.size() will be 0
                //and below while loop will be infinite loop;
                //timeSeries.size() = 1 means that there is only instant there,
                //in this case otherTimeSeries also has one instant, so return the distance between them
                if(timeSeries.size() == 1) // otherTimeSeries.size() is also equal to 1
                    return timeSeries.get(0).distanceInSecondsTo(other.getTimeSeries().get(0));


                //At this point, timeSeries contains at least two instants, therefore custom looping method will work;
                //Assume that B_3 = (s_0, s_1, s_2, s_3), B_10 = (s_7, s_8, s_9, s_10)
                //d(B_3, B_10) will be calculated as follows:
                //1) Compute comparative distances and store them:
                //Store d_1_0 = d(s_1, s_0), d_2_1 = d(s_2, s_1), d_3_2 = d(s_3, s_2) in one array list
                //Store d_8_7 = d(s_8, s_7), d_9_8 = (s_9, s_8), d_10_9 = d(s_10, s_9) in the other array list
                //2) Compute comparative distances and store them:
                //Store d_21_10 = |d_2_1 - d_1_0|, d_32_21 = |d_3_2 - d_2_1|
                //Store d_98_97 = |d_9_8 - d_8_7|, d_109_98 = |d_10_9 - d_9_8|
                //3) Compute comparative distances and store them:
                //Store d_x = |d_21_10 - d_32_21|
                //Store d_y = |d_98_87 - d_109_98|
                //4) Compute final distance using:
                //D = d_x - d_y
                //ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                //ArrayList<Double> thisComparativeDistances = new ArrayList<Double>();
                //ArrayList<Double> otherComparativeDistances = new ArrayList<Double>();
                //for (int index = 0; index < timeSeries.size() - 1; index++)
                //{
                //    //do comparative distance calculation for the first embedding vector
                //    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                //    //add it to the first array list
                //    thisComparativeDistances.add(thisComparativeDistance);
                //
                //    //now do the the same thing for the other embedding vector
                //    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                //    //add it to the second array list
                //    otherComparativeDistances.add(otherComparativeDistance);
                //} // for

                ArrayList<TimeSeriesInstant> otherTimeSeries = other.getTimeSeries();
                double[] thisComparativeDistances = new double[timeSeries.size() - 1];
                double[] otherComparativeDistances = new double[timeSeries.size() - 1];
                for(int index = 0; index < timeSeries.size() - 1; index ++)
                {
                    //do comparative distance calculation for the first embedding vector
                    double thisComparativeDistance = timeSeries.get(index).distanceInSecondsTo(timeSeries.get(index + 1));
                    //add it to the first array
                    thisComparativeDistances[index] = thisComparativeDistance;

                    //now do the the same thing for the other embedding vector
                    double otherComparativeDistance = otherTimeSeries.get(index).distanceInSecondsTo(otherTimeSeries.get(index + 1));
                    //add it to the second array
                    otherComparativeDistances[index] = otherComparativeDistance;
                } // for
                //------------------------- SIMILAR FOR ALL CUSTOM DISTANCES ------------------------


                //recursive comparative distance on already calculated distances
                //return recursiveComparativeDistance(thisComparativeDistances, otherComparativeDistances);
                //recursive method above is re-written using a while loop

                while (true)
                {
                    //System.out.println("timeSeries.size() = " + timeSeries.size());
                    //System.out.println("thisComparativeDistances.size() = " + thisComparativeDistances.size());

                    //thisComparativeDistances and otherComparativeDistances have the same size
                    //recursion will stop when there is only one element left in each array list
                    if (thisComparativeDistances.length /*.size()*/ == 1) // if it is 1, then otherComparativeDistances.size() is also 1
                    {
                        //thisComparativeDistances => {d_x}; otherComparativeDistances => {d_y}
                        //just subtract d_x and d_y and take its absolute value
                        return Math.abs(
                                //thisComparativeDistances.get(0) - otherComparativeDistances.get(0)
                                thisComparativeDistances[0] - otherComparativeDistances[0]
                                );
                    } // if


                    //otherwise generate a new array lists of comparative distances
                    //ArrayList<Double> first = new ArrayList<Double>();
                    //ArrayList<Double> second = new ArrayList<Double>();

                    double[] first = new double[thisComparativeDistances.length - 1];
                    double[] second = new double[thisComparativeDistances.length - 1];

                    //if we have the following:
                    //thisComparativeDistances => {d_1, d_2, d_3}; otherComparativeDistances => (d_4, d_5, d_6)
                    //Then new reconstructed array list will contain the following:
                    //first => (|d_1 - d_2|, |d_2 - d_3|); second => {|d_4 - d_5|, |d_5 - d_6|}
                    //and this method will be recursively called with reconstructed array lists
                    for (int index = 0; index < thisComparativeDistances.length /*size()*/ - 1; index++)
                    {
                        //do comparative distance calculation for the first given array list
                        double firstComparativeDistance
                                = Math.abs(
                                        //thisComparativeDistances.get(index) - thisComparativeDistances.get(index + 1)
                                        thisComparativeDistances[index] - thisComparativeDistances[index + 1]
                                        );

                        //add it to the first reconstructed array list
                        ///first.add(firstComparativeDistance);
                        first[index] = firstComparativeDistance;

                        //now do the the same thing for the the second given array list
                        double secondComparativeDistance
                                = Math.abs(
                                        //otherComparativeDistances.get(index) - otherComparativeDistances.get(index + 1)
                                        otherComparativeDistances[index] - otherComparativeDistances[index + 1]
                                        );
                        //add it to the second reconstructed array list
                        //second.add(secondComparativeDistance);
                        second[index] = secondComparativeDistance;
                    } // for


                    //assign first and second accordingly
                    thisComparativeDistances = first;
                    otherComparativeDistances = second;
                } // while
            } // case ComparativeLoopingDistance


            //default is manhattan
            default:
            {
                long cumulativeDistance = 0;

                for (int index = 0; index < timeSeries.size(); index++) {
                    cumulativeDistance += timeSeries.get(index).distanceInSecondsTo(other.getTimeSeries().get(index));
                } // for

                return cumulativeDistance;
            } // default

            } // switch

    } // distanceTo



    /*
    //helper method to find recursive comparative distance on given distances
    private double recursiveComparativeDistance(ArrayList<Double> thisComparativeDistances, ArrayList<Double> otherComparativeDistances)
    {
        //thisComparativeDistances and otherComparativeDistances have the same size
        //recursion will stop when there is only one element left in each array list
        if(thisComparativeDistances.size() == 1) // if it is 1, then otherComparativeDistances.size() is also 1
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
        for(int index = 0; index < thisComparativeDistances.size() - 1; index ++)
        {
            //do comparative distance calculation for the first given array list
            double firstComparativeDistance = Math.abs(thisComparativeDistances.get(index) - thisComparativeDistances.get(index + 1));
            //add it to the first reconstructed array list
            firstList.add(firstComparativeDistance);

            //now do the the same thing for the the second given array list
            double secondComparativeDistance = Math.abs(otherComparativeDistances.get(index) - otherComparativeDistances.get(index + 1));
            //add it to the second reconstructed array list
            secondList.add(secondComparativeDistance);
        } // for

        //recursively call the same method with reconstructed array lists
        return recursiveComparativeDistance(firstList, secondList);
    } // recursiveComparativeDistance
    */


    //getter method for time series
    public ArrayList<TimeSeriesInstant> getTimeSeries() {
        return timeSeries;
    }


    //hash code is the same as the hash code of its time series
    @Override
    public int hashCode()
    {
        return timeSeries.hashCode();
    } // hashCode


    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof EmbeddingVector))
            return false;
        else if(this == other)
            return true;
        else return timeSeries.equals(((EmbeddingVector) other).getTimeSeries());
    } // equals

    //toString method to print out all s_i inside this embedding vector betta
    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for(TimeSeriesInstant instant : timeSeries)
            stringBuilder.append(instant + "\n");

        return stringBuilder.toString();
    } // toString

    public String toStringWithDate()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for(TimeSeriesInstant instant : timeSeries)
            stringBuilder.append(instant.toStringWithDate() + "\n");

        return stringBuilder.toString();
    } // toStringWithDate

} // class EmbeddingVector
