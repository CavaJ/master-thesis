package com.rb.nextplace.distributed;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Rufat Babayev on 2/12/2018.
 */

//Because enums are automatically Serializable (according to Javadoc API documentation for Enum), there is no need to
//explicitly add the "implements Serializable" clause following the enum declaration
public enum Dataset implements Serializable
{
    Cabspotting,
    CenceMeGPS,
    DartmouthWiFi,
    IleSansFils;

    public long traceLengthInDays()
    {
        switch(this)
        {
            case Cabspotting:
                try
                {
                    Date firstDate = Utils.toDate("2008-05-17", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2008-06-10", "yyyy-MM-dd");
                    return Utils.noOfDaysBetween(firstDate, secondDate);
                }
                catch(ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch

            case CenceMeGPS:
                try
                {
                    Date firstDate = Utils.toDate("2008-07-23", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2008-08-13", "yyyy-MM-dd");
                    return Utils.noOfDaysBetween(firstDate, secondDate);
                } // try
                catch(ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch

            case DartmouthWiFi:
                try
                {
                    //Details of movement/infocom04 trace:
                    //Min measurement date: Wed Apr 11 16:57:27 AZST 2001
                    //Max measurement date: Sun Mar 16 08:59:56 AZT 2003
                    //Trace length (number of measurement days): 703

                    Date firstDate = Utils.toDate("2001-04-11", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2004-07-01", "yyyy-MM-dd"); //"2006-10-04");
                    return Utils.noOfDaysBetween(firstDate, secondDate);
                } // try
                catch(ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch

            case IleSansFils:
                try
                {
                    Date firstDate = Utils.toDate("2004-08-28", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2007-08-28", "yyyy-MM-dd");
                    return Utils.noOfDaysBetween(firstDate, secondDate);
                } // try
                catch(ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch

                //for unknown datasets return 0
                default: return 0;
        } // switch

    } // traceLengthInDays

    public void printInfo()
    {
        switch (this) {
            case Cabspotting:
                //Dataset information of Cabspotting dataset
                try
                {
                    System.out.println("\n------- Cabspotting dataset of " + getNoOfUsers() + " taxi cabs (users) -------");
                    System.out.println("-------------------------------------------------------------");
                    Date firstDate = Utils.toDate("2008-05-17", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2008-06-10", "yyyy-MM-dd");
                    System.out.println("Measurements are performed between: ");
                    System.out.println(Utils.toDateStringWithoutTime(firstDate)
                            + "\n"
                            + Utils.toDateStringWithoutTime(secondDate));
                    System.out.println("No of days of measurement: " + Utils.noOfDaysBetween(firstDate, secondDate));
                    System.out.println("-------------------------------------------------------------");
                } // try
                catch (ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch
                break;
            case CenceMeGPS:
                //Dataset information of CenceMeGPS dataset
                try
                {
                    System.out.println("\n------- CenceMeGPS dataset of " + getNoOfUsers() + " Nokia N95 users -------");
                    System.out.println("--------------------------------------------------------");
                    Date firstDate = Utils.toDate("2008-7-23", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2008-08-13", "yyyy-MM-dd");
                    System.out.println("Measurements are performed between: ");
                    System.out.println(Utils.toDateStringWithoutTime(firstDate)
                            + "\n"
                            + Utils.toDateStringWithoutTime(secondDate));
                    System.out.println("No of days of measurement: " + Utils.noOfDaysBetween(firstDate, secondDate));
                    System.out.println("--------------------------------------------------------");
                } // try
                catch (ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch
                break;
            case DartmouthWiFi:
                //Dataset information of DartmouthWiFi dataset
                try
                {
                    //Details of movement/infocom04 trace:
                    //Min measurement date: Wed Apr 11 16:57:27 AZST 2001
                    //Max measurement date: Sun Mar 16 08:59:56 AZT 2003
                    //Trace length (number of measurement days): 703

                    System.out.println("\n------- DartmouthWiFi dataset of 3200-3300 undergraduate students at Dartmouth College -------");
                    System.out.println("----------------------------------------------------------------------------------------------");
                    Date firstDate = Utils.toDate("2001-04-11", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2004-07-01", "yyyy-MM-dd"); //"2006-10-04");
                    System.out.println("Measurements are performed between: ");
                    System.out.println(Utils.toDateStringWithoutTime(firstDate)
                            + "\n"
                            + Utils.toDateStringWithoutTime(secondDate));
                    System.out.println("No of days of measurement: " + Utils.noOfDaysBetween(firstDate, secondDate));
                    System.out.println("----------------------------------------------------------------------------------------------");
                } // try
                catch (ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch
                break;
            case IleSansFils:
                //Dataset information of IleSansFils dataset
                try
                {
                    System.out.println("\n------- IleSansFils dataset of over 45,000 users from free Wi-Fi hotspots in Montreal, Quebec -------");
                    System.out.println("-----------------------------------------------------------------------------------------------------");
                    Date firstDate = Utils.toDate("2004-08-28", "yyyy-MM-dd");
                    Date secondDate = Utils.toDate("2007-08-28", "yyyy-MM-dd");
                    System.out.println("Measurements are performed between: ");
                    System.out.println(Utils.toDateStringWithoutTime(firstDate)
                            + "\n"
                            + Utils.toDateStringWithoutTime(secondDate));
                    System.out.println("No of days of measurement: " + Utils.noOfDaysBetween(firstDate, secondDate));
                    System.out.println("-----------------------------------------------------------------------------------------------------");
                } // try
                catch (ParseException ex)
                {
                    System.err.println(ex.toString());
                    ex.printStackTrace();
                } // catch
                break;
            default:
                throw new AssertionError("Unknown dataset info " + this);
        }
    } // printInfo

    @Override
    public String toString()
    {
        switch (this)
        {
            case Cabspotting:
                return "Cabspotting dataset";
            case CenceMeGPS:
                return "CenceMe GPS dataset";
            case DartmouthWiFi:
                return "Dartmouth WiFi dataset";
            case IleSansFils:
                return "Ile Sans Fils dataset";
            default:
                return "Unknown dataset";
        } // switch
    } // toString

    //simple string representation of datasets
    public String toSimpleString()
    {
        switch (this)
        {
            case Cabspotting:
                return "cabspotting";
            case CenceMeGPS:
                return "cencemegps";
            case DartmouthWiFi:
                return "dartmouthwifi";
            case IleSansFils:
                return "ilesansfils";
            default:
                return "unknown";
        } // switch
    } // toSimpleString


    //name of the dataset
    public String getName()
    {
        switch (this)
        {
            case Cabspotting:
                return "Cabspotting";
            case CenceMeGPS:
                return "CenceMeGPS";
            case DartmouthWiFi:
                return "DartmouthWiFi";
            case IleSansFils:
                return "IleSansFils";
            default:
                return "Unknown";
        } // switch
    } // getName


    //method to get the root tag name
    //of global xml file of the dataset which contains
    //user file names to run the algorithm on
    public String rootTagName()
    {
        //adjust rootTagName based on dataset
        switch (this)
        {
            case Cabspotting:
                //root tag name is "cabs" for Cabspotting dataset
                return "cabs";
            case CenceMeGPS:
                //root tag name is "users" for CenceMeGPS dataset
                return "users";
            case DartmouthWiFi:
                //unknown for DartmouthWiFi
                return "unknown";
            case IleSansFils:
                //unknown for IleSansFils
                return "unknown";
            default:
                //and unknown for default
                return "unknown";
        } // switch
    } // rootTagName


    //method to get the user tag name
    //of global xml file of the dataset which contains
    //user file names to run the algorithm on
    public String userTagName()
    {
        //adjust userTagName based on dataset
        switch (this)
        {
            case Cabspotting:
                //user tag name is "cab" for Cabspotting dataset
                return "cab";
            case CenceMeGPS:
                //user tag name is "user" for CenceMeGPS dataset
                return "user";
            case DartmouthWiFi:
                //unknown for DartmouthWiFi
                return "unknown";
            case IleSansFils:
                //unknown for IleSansFils
                return "unknown";
            default:
                //and unknown for default
                return "unknown";
        } // switch
    } // userTagName

    //method to get the user attribute name of user tag
    //of global xml file of the dataset which contains
    //user file names to run the algorithm on
    public String userAttributeName()
    {
        //adjust userAttributeName based on dataset
        switch (this)
        {
            case Cabspotting:
                //user attribute name is "id" for Cabspotting dataset
                return "id";
            case CenceMeGPS:
                //user attribute name is "filename" for CenceMeGPS dataset
                return "filename";
            case DartmouthWiFi:
                //unknown for DartmouthWiFi
                return "unknown";
            case IleSansFils:
                //unknown for IleSansFils
                return "unknown";
            default:
                //and unknown for default
                return "unknown";
        } // switch
    } // userAttributeName


    //helper method to return number of users of datasets
    public int getNoOfUsers()
    {
        switch(this)
        {
            case Cabspotting:
                return 536;
            case CenceMeGPS:
                return 20;
            case DartmouthWiFi:
                return 13888; // meaning unknown
            case IleSansFils:
                return 69689; // meaning unknown
            default:
                return 0; // meaning unknown
        } // switch
    } // getNoOfUsers

} // enum Dataset
