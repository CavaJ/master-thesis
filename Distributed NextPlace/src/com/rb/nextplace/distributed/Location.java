package com.rb.nextplace.distributed;

import org.apache.commons.math3.ml.clustering.Clusterable;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Created by Rufat Babayev on 2/4/2018.
 */
public class Location implements Serializable, Comparable<Location>, Clusterable
{
    //implemented at least 5 decimal place rounding when location is created
    //decimal  degrees    distance
    //-------------------------------
    //0        1.0        111 km
    //1        0.1        11.1 km
    //2        0.01       1.11 km
    //3        0.001      111 m
    //4        0.0001     11.1 m
    //5        0.00001    1.11 m
    //6        0.000001   0.111 m
    //7        0.0000001  1.11 cm
    //8        0.00000001 1.11 mm

    //generated serial version UID
    private static final long serialVersionUID = 6604549336464120828L;

    //constants for min and max latitudes and longitudes
    public static final double MAX_LATITUDE = 90;
    public static final double MAX_LONGITUDE = 180;
    public static final double MIN_LATITUDE = -90;
    public static final double MIN_LONGITUDE = -180;


    //location name
    private String name;
    //latitude and longitude of this location
    private double latitude;
    private double longitude;


    //main constructor
    //create and initialize a point with given name and
    //(latitude, longitude) specified in degrees
    public Location(String name, double latitude, double longitude)
    {
        this.name = name;
        this.latitude  = latitude;
        this.longitude = longitude;
    } // Location


    //return string representation of this point
    public String toString() { return name + "[lat: " + latitude + ", long: " + longitude + "]"; } // toString

    //return simple string representation of this location
    public String toSimpleString() { return latitude + "," + longitude; }


    //this will call main constructor with UNK -> unknown name
    public Location(double latitude, double longitude)
    {
        this("UNK", latitude, longitude);
    } // Location

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Location))
        {
            System.err.println("Object other is not of type Location");
            return false;
        } // if
        else if(this == other)
        {
            return true;
        } // else
        else
        {
            //1) Compare latitude and longitude with reliable Double.compare()
            //Double.compare method is more reliable since it uses Double.doubleToLongBits()
            return this.name.equals(((Location) other).name) // consistent with String.compareTo()
                            && Double.compare(this.latitude, ((Location) other).latitude) == 0
                            && Double.compare(this.longitude, ((Location) other).longitude) == 0;
        } // else
    } // equals



    //int precision = 12;



    //if two objects are equal by equals() then their hashCode() methods should produce the same integer result
    @Override
    public int hashCode()
    {
        //Objects.hash() method is useful for implementing Object.hashCode() on objects containing multiple fields.
        //Objects.hash() calls Double's hashCode() which is more reliable, since it uses Double.doubleToLongBits()
        //return Objects.hash(latitude, longitude);


        //above Objects.hash() does this:
        //int prime = 31;
        //int result = 1;
        //result = prime * result + ((object1 == null) ? 0 : object1.hashCode());
        //result = prime * result + ((object2 == null) ? 0 : object2.hashCode());
        //return result;


        //We can rewrite Objects.hash() call more efficiently (without autoboxing and varargs to array conversion) as follows:
        int prime = 31;
        int result = 17; //1; => 1 is in the Objects.hash()
        result = prime * result + name.hashCode();
        result = prime * result + Double.hashCode(latitude);
        result = prime * result + Double.hashCode(longitude);
        return result;

        //1) another is described here:
        //https://stackoverflow.com/questions/11742593/what-is-the-hashcode-for-a-custom-class-having-just-two-int-properties
    } // hashCode


    //The valid range of latitude in degrees is >> -90 and +90 << for the southern and northern hemisphere respectively.
    //Longitude is in the range >> -180 and +180 << specifying coordinates west and east of the Prime Meridian, respectively.
    @Override
    public int compareTo(Location other)
    {
        //if names are equal, then compare with latitudes and longitudes
        //if latitudes are equal, then compare with longitudes
        //if latitudes are different, then compare with latitudes
        String locName = this.name;
        Double lat = this.latitude;
        Double lng = this.longitude;


        //more clear version of three-component compareTo
        int result = locName.compareTo(other.getName());
        if(result == 0)
        {
            result = lat.compareTo(other.getLatitude());
            if(result == 0)
                result = lng.compareTo(other.getLongitude());
        } // if
        return result;
    } // compareTo


    //returns distance in meters between this location and other location
    /**
     * Calculate the surface distance in meters from this Location to the given
     * Location.
     */
    public double distanceTo(Location other)
    {
        //if this location and other location is the same, then return 0
        if(this.equals(other)) return 0;


        //otherwise compute the earth distance between locations
        double earthRadius = 3958.7558657441; // earth radius in miles
        double dLat = Math.toRadians(other.getLatitude() - this.latitude);
        double dLng = Math.toRadians(other.getLongitude() - this.longitude);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(this.latitude))
                * Math.cos(Math.toRadians(other.getLatitude())) * Math.sin(dLng / 2)
                * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double dist = earthRadius * c;

        double meterConversion = 1609.344;

        return dist * meterConversion;
    } // distanceTo


    public double distanceInMiles(Location other)
    {
        return distanceTo(other) / 1609.344;
    } // distanceInMiles


    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getName() {return name; }



    //Gets the n-dimensional point.
    @Override
    public double[] getPoint()
    {
        return new double[] {this.latitude, this.longitude};
    } // getPoint


    //static inner class for location tuples to calculate the gaussian
    //this is bi-object which means position of first and second does not matter
    public static class Location2Tuple implements Serializable, Comparable<Location2Tuple>
    {
        private static final long serialVersionUID = 7529712007603086482L;

        private Location first;
        private Location second;

        public Location2Tuple(Location first, Location second)
        {
            this.first = first;
            this.second = second;
        } // Location2Tuple


        //string representation
        @Override
        public String toString()
        {
            return "Location2Tuple{" + first + " <=> " + second + "}";
        } // toString


        //hashcode method
        @Override
        public int hashCode()
        {
            //perform XOR => 5 ^ 6 = 3 and 6 ^ 5 = 3
            return first.hashCode() ^ second.hashCode();
        } // hashCode

        //corresponding equals method
        @Override
        public boolean equals(Object other)
        {
            if(!(other instanceof Location2Tuple))
            {
                System.err.println("Object other is not of type Location2Tuple");
                return false;
            } // if
            else if(this == other)
            {
                return true;
            } // else
            else
            {
                //the positions does not matter
                return this.hashCode() == ((Location2Tuple) other).hashCode();
            } // else
        } // equals


        @Override
        public int compareTo(Location2Tuple other)
        {
            //if hash codes are the same return 0, otherwise perform location-wise comparison
            if(this.hashCode() == ((Location2Tuple) other).hashCode()) return 0;

            int result1 = first.compareTo(other.first);
            int result2 = first.compareTo(other.second);
            int result3 = second.compareTo(other.second);
            int result4 = second.compareTo(other.first);


            if(result1 == 0)
                return result3;
            else if(result2 == 0)
                return result4;
            else if(result3 == 0)
                return result1;
            else if(result4 == 0)
                return result2;
            else if(result1 + result3 < result2 + result4)
                return result1 + result3;
            else
                return result2 + result4;
        } // compareTo
    } // class Location2Tuple


    public static class NaturalOrderComparator implements Comparator<Location>
    {

        @Override
        public int compare(Location o1, Location o2) {
            return o1.compareTo(o2);
        } // compare
    }

} // class Location
