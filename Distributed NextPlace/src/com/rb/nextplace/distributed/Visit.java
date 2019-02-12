package com.rb.nextplace.distributed;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * Created by Rufat Babayev on 2/4/2018.
 */
public class Visit implements Comparable<Visit>, Serializable
{
    //generated serial version UID
    private static final long serialVersionUID = 3145501243299465051L;

    //visit's user name, location and time series instant
    private String userName;
    private Location location;
    private TimeSeriesInstant timeSeriesInstant;

    public Visit(String userName, Location location, TimeSeriesInstant timeSeriesInstant)
    {
        this.userName = userName;
        this.location = location;
        this.timeSeriesInstant = timeSeriesInstant;
    } // Visit

    @Override
    public String toString()
    {
        return "{" + userName + "; " + location + "; " + timeSeriesInstant + "}";
    } // toString

    //toString method with date
    public String toStringWithDate()
    {
        return "{" + userName + "; " + location + "; " + timeSeriesInstant.toStringWithDate() + "}";
    } // toStringWithDate


    //toString method with time (which is a Date without day, month, year)
    public String toStringWithTime()
    {
        return "{" + userName + "; " + location + "; " + timeSeriesInstant.toStringWithTime() + "}";
    } // toStringWithTime


    //however, visits will be sorted chronologically avoiding equality of user names and locations
    @Override
    public int compareTo(Visit other)
    {
        return this.timeSeriesInstant.compareTo(other.timeSeriesInstant);
    } // compareTo

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Visit))
            return false;
        else if(this == other)
            return true;
        else
            //visits are only equal when they belong to the same user, have the same location
            //and performed at the same (chronological) arrival date and time as provided by its time series instant
            return this.getUserName().equals((((Visit) other).getUserName()))
                    && this.getLocation().equals(((Visit) other).getLocation())
                    && this.compareTo((Visit) other) == 0;
    } // equals


    //corresponding hashCode method which uses all there fields of Visit class
    @Override
    public int hashCode()
    {
        //return the result of convenience method at Objects class
        return Objects.hash(userName, location, timeSeriesInstant);
    } // hashCode


    public String getUserName() { return userName; }

    public Location getLocation() {
        return location;
    }

    public TimeSeriesInstant getTimeSeriesInstant() {
        return timeSeriesInstant;
    }


    //helper method to merge one visit with the other one
    public Visit mergeWith(Visit other)
    {
        //new visit will take the location and arrival time of "this" visit
        //and residence will be calculated on the residence of the other visit
        //and "this" visit
        //so, new d_i = t_i+1 - t_i, since t_i+1 - t_i contains previous merged visit's d_i
        Visit mergedVisit;

        //get arrival times in Date format
        Date thisArrivalTime = this.getTimeSeriesInstant().getArrivalTime();
        Date otherArrivalTime = other.getTimeSeriesInstant().getArrivalTime();

        //if they are on the same day, it is OK to subtract arrival time of day seconds of this
        //from the arrival time of day seconds of other or vice versa
        if(Utils.onTheSameDay(thisArrivalTime, otherArrivalTime))
        {
            if (this.before(other))
            {
                TimeSeriesInstant newInstant = new TimeSeriesInstant(thisArrivalTime,
                        other.getTimeSeriesInstant().getArrivalTimeInDaySeconds()   // t_i+1 - t_i
                                - this.getTimeSeriesInstant().getArrivalTimeInDaySeconds() //); //OLD

                                + other.getTimeSeriesInstant().getResidenceTimeInSeconds());            // + d_i+1
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // if
            else if (this.after(other))
            {
                //"other" is before "this"
                TimeSeriesInstant newInstant = new TimeSeriesInstant(otherArrivalTime,
                        this.getTimeSeriesInstant().getArrivalTimeInDaySeconds()    // t_i+1 - t_i
                                - other.getTimeSeriesInstant().getArrivalTimeInDaySeconds() //); //OLD

                                + this.getTimeSeriesInstant().getResidenceTimeInSeconds());             // + d_i+1
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // else if
            else // else they are equal
            {
                //it can be possible to create instance with the details of "this" or "other"
                //this is an impossible case in time series
                //such that t_i = t_i+1 which means the same day seconds on the same day, therefore we return (t_i, d_i)
                TimeSeriesInstant newInstant = new TimeSeriesInstant(thisArrivalTime,
                        this.getTimeSeriesInstant().getResidenceTimeInSeconds());
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // else
        } // if
        // they are not in the same day;
        //if one visit is on one day and the other one is on the next day;
        //apply residenceTimeInSeconds = nextVisitDaySeconds + 86400 - lastVisitDaySeconds
        //but mergeWith() method is called from method which checks noOfDaysBetween and this is case when noOfDaysBetween = 0.
        //for outer calls, this method will assume this and other as subsequent days or vice versa for below else clause
        else
        {
            //But, if they are in different but subsequent days, such as:
            //Date:     Arr:    Res:
            //March 8   86330    100
            //March 9   70    0
            //---------------------------
            //then new residence seconds will be
            if(this.before(other))
            {
                long newResidenceSeconds = other.getTimeSeriesInstant().getArrivalTimeInDaySeconds()          // t_i+1 - t_i
                                                + 86400 - this.getTimeSeriesInstant().getArrivalTimeInDaySeconds()  //; //OLD

                                                + other.getTimeSeriesInstant().getResidenceTimeInSeconds();   // + d_i+1
                TimeSeriesInstant newInstant = new TimeSeriesInstant(thisArrivalTime, newResidenceSeconds);
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // if
            else if(this.after(other)) //"other" is before "this"
            {
                long newResidenceSeconds = this.getTimeSeriesInstant().getArrivalTimeInDaySeconds()                // t_i+1 - t_i
                                                + 86400 - other.getTimeSeriesInstant().getArrivalTimeInDaySeconds() //; //OLD

                                                + this.getTimeSeriesInstant().getResidenceTimeInSeconds();         // + d_i+1
                TimeSeriesInstant newInstant = new TimeSeriesInstant(otherArrivalTime, newResidenceSeconds);
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // else
            else        //since this state is not possible, Dates on different days cannot be equal
            {
                //but we have created the following statement for convenience
                //lets say if we discard Date information;
                //in this case we can have t_i == t_i+1, but those days cannot be merged,
                //since merge deltaThreshold can e.g. 60, 180, 300 seconds which cannot be 1 day ( (24 * 3600) seconds)
                TimeSeriesInstant newInstant = new TimeSeriesInstant(thisArrivalTime,   // choose as (t_i, d_i)
                        this.getTimeSeriesInstant().getResidenceTimeInSeconds());
                mergedVisit = new Visit(this.getUserName(), this.getLocation(), newInstant);
            } // else
        } // else

        return mergedVisit;
    } // mergeWith

    //helper method to check whether one visit is before the other
    public boolean before(Visit other)
    {
        return this.getTimeSeriesInstant().before(other.getTimeSeriesInstant());
    } // before


    //helper method to check whether one visit is after the other
    public boolean after(Visit other)
    {
        return this.getTimeSeriesInstant().after(other.getTimeSeriesInstant());
    } // after


    //helper method to create new instance from a visit
    //the contents of the visits will be the same
    //however, they will be different objects;
    //this method will be needed during preprocessing
    public static Visit newInstance(Visit visit)
    {
        TimeSeriesInstant newInstant = new TimeSeriesInstant(visit.getTimeSeriesInstant().getArrivalTime(),
                visit.getTimeSeriesInstant().getResidenceTimeInSeconds());
        return new Visit(visit.getUserName(), visit.getLocation(), newInstant);
    } // newInstance


    //helper method to generate a Visit whose date is Unix epoch
    public static Visit newInstanceWithUnixEpochDate(Visit v)
    {
        long arrivalTimeInDaySeconds = v.getTimeSeriesInstant().getArrivalTimeInDaySeconds();
        Date newArrivalTime = Utils.unixEpochDateWithDaySeconds(arrivalTimeInDaySeconds);
        TimeSeriesInstant newInstant = new TimeSeriesInstant(newArrivalTime, v.getTimeSeriesInstant().getResidenceTimeInSeconds());
        return new Visit(v.getUserName(), v.getLocation(), newInstant);
    } // newInstanceWithUnixEpochDate
} // class Visit
