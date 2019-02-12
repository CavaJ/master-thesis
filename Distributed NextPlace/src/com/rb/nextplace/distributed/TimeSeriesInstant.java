package com.rb.nextplace.distributed;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Rufat Babayev on 2/4/2018.
 */
public class TimeSeriesInstant implements Comparable<TimeSeriesInstant>, Serializable
{
    //generated serial version UID
    private static final long serialVersionUID = 2434044911547574794L;

    //instance variables
    private Date arrivalTime; // will be used to sort time series instants chronologically
    private long arrivalTimeInDaySeconds;

    private long residenceTimeInSeconds;

    //index will only be updated when timeSeriesInstant is added to the visitHistory
    //along with its encapsulating visit
    //initially the index will be -1; meaning that this instant is not added to the visitHistory
    private int instantIndex;

    //boolean value to check whether the time series instant is actually a prediction
    private boolean isPrediction;

    public void setPrediction(boolean prediction) { isPrediction = prediction; }
    public boolean isPrediction() { return isPrediction; }
    //public void setInstantIndex(int instantIndex) { this.instantIndex = instantIndex; }

    /**
     * Constructor method to create time series instant from arrival date and residence minutes
     * @param arrivalTimeInDaySeconds arrival time of the user to a specific location in day seconds
     * @param residenceTimeInSeconds number of presence seconds of the user in a specific location
     */
    private TimeSeriesInstant(long arrivalTimeInDaySeconds, long residenceTimeInSeconds)
    {
        this.arrivalTimeInDaySeconds = arrivalTimeInDaySeconds;
        this.residenceTimeInSeconds = residenceTimeInSeconds;

        instantIndex = -1;
        isPrediction = false;
    } // TimeSeriesInstant

    /**
     * Constructor method to create time series instant from arrival date and residence minutes
     * @param arrivalTime arrival time of the user to a specific location with Date type
     * @param residenceTimeInSeconds number of presence seconds of the user in a specific location
     */
    public TimeSeriesInstant(Date arrivalTime, long residenceTimeInSeconds)
    {
        this(Utils.toDaySeconds(arrivalTime), residenceTimeInSeconds);
        this.arrivalTime = arrivalTime;
    } // TimeSeriesInstant



    //in the ORIGINAL PAPER similarity and distance is based on arrivalTimeInDaySeconds;
    //distance between two time series instants e.g. number of seconds between them
    public long distanceInSecondsTo(TimeSeriesInstant other)
    {
        //simply return absolute value of arrival time in day seconds of time series instants
        return Math.abs(this.arrivalTimeInDaySeconds - other.getArrivalTimeInDaySeconds());
    } // distanceInSecondsTo



    //helper method to detect whether this time series instance is before the other
    public boolean before(TimeSeriesInstant other)
    {
        return this.getArrivalTime().before(other.getArrivalTime());
    } // before


    //to check whether the this instant is before or equals other instant
    public boolean beforeOrEquals(TimeSeriesInstant other)
    {
        return this.equals(other) || this.before(other);
    } // beforeOrEquals


    //to check whether the this instant is after or equals other instant
    public boolean afterOrEquals(TimeSeriesInstant other)
    {
        return this.equals(other) || this.after(other);
    } // beforeOrEquals


    //helper method to detect whether this time series instance is after the other
    public boolean after(TimeSeriesInstant other)
    {
        return this.getArrivalTime().after(other.getArrivalTime());
    } // after


    /*
    //method for adding seconds to the arrival time in day seconds of this time series instant
    public TimeSeriesInstant getInstantWithSecondsAddedToArrivalTimeDaySeconds(long seconds)
    {
        Date secondsAddedArrivalTime = Utils.addSeconds(this.getArrivalTime(), (int) seconds);
        return new TimeSeriesInstant(secondsAddedArrivalTime, this.getResidenceTimeInSeconds());
    } // addSecondsToArrivalTimeInDaySeconds
    */


    //compareTo should be performed based on Date
    /*
    @Override
    public int compareTo(TimeSeriesInstant other)
    {
        return (int) (this.arrivalTimeInDaySeconds - other.arrivalTimeInDaySeconds);
    } // compareTo
    */


    //comparison based on Date will make the sensor reading chronologically stable
    //this will guarantee order of sensor readings since the first of GPS or Wi-Fi tracking
    @Override
    public int compareTo(TimeSeriesInstant other)
    {
        return this.arrivalTime.compareTo(other.arrivalTime);
    } // compareTo


    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof TimeSeriesInstant))
            return false;
        else if(this == other)
            return true;
        else
            return this.compareTo((TimeSeriesInstant) other) == 0;
    } // equals


    //corresponding hashCode method; objects which is equals by equals() should have the same int hashCode
    @Override
    public int hashCode()
    {
        //equals method uses compareTo() method which in turn uses Date.compareTo(), therefore using Date.hashCode seems reasonable
        //and in fact if the <Date arrivalTime> is the same then <long arrivalTimeInDaySeconds> are also same if we check equality
        //only residenceTimeInSeconds can be different which is negligible
        return this.arrivalTime.hashCode();
    } // hashCode


    @Override
    public String toString()
    {
        String notation = isPrediction ? "P_" : "S_";
        return notation + instantIndex + "(arr: " + arrivalTimeInDaySeconds + ", res: " + residenceTimeInSeconds + ")";
        //return "(arr: " + arrivalTimeInDaySeconds + ", res: " + residenceTimeInSeconds + ")";
    } // toString


    //toString method with Date
    public String toStringWithDate()
    {
        String notation = isPrediction ? "P_" : "S_";
        return notation + instantIndex + "[" + arrivalTime + "]" + "(arr: " + arrivalTimeInDaySeconds + ", res: " + residenceTimeInSeconds + ")";
    } // toStringWithDate


    //toString method with time (which is a Date without day, month, year)
    public String toStringWithTime()
    {
        String notation = isPrediction ? "P_" : "S_";
        return notation + instantIndex + "[" +
                new SimpleDateFormat("HH:mm:ss zzz").format(arrivalTime)
                + "]" + "(arr: " + arrivalTimeInDaySeconds + ", res: " + residenceTimeInSeconds + ")";
    } // toStringWithTime

    //getter method for arrival time
    public Date getArrivalTime() {
        return arrivalTime;
    }

    //getter method for arrival time in day seconds
    public long getArrivalTimeInDaySeconds() {
        return arrivalTimeInDaySeconds;
    }

    //getter method for residence time in seconds
    public long getResidenceTimeInSeconds() {
        return residenceTimeInSeconds;
    }

    public void setResidenceTimeInSeconds(long residenceTimeInSeconds) {
        this.residenceTimeInSeconds = residenceTimeInSeconds;
    }

} // class TimerSeriesInstant