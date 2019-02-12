package com.rb.nextplace.distributed;

import com.google.common.collect.*;

import java.math.RoundingMode;
import java.util.*;

public class Testing
{
    //helper method to test 0 residence seconds in visits of user files of datasets
    private static void testZeroResidenceSeconds(Dataset dataset,
                                                 String localPathString,
                                                 boolean listLocalFilesRecursively)
    {
        System.out.println("Checking " + dataset + " for possible visits having zero residence seconds");
        List<String> namesOfFilesHavingZeroResSecVisits = new ArrayList<>();

        List<String> ppUserFilePaths = Utils.listFilesFromLocalPath(localPathString, listLocalFilesRecursively);
        for(String ppUserFilePath: ppUserFilePaths)
        {
            String fileContents = Utils.fileContentsFromLocalFilePath(ppUserFilePath);
            ArrayList<Visit> mergedVisits = Utils.visitsFromPPUserFileContents(fileContents);

            for(Visit v : mergedVisits)
            {
                if(v.getTimeSeriesInstant().getResidenceTimeInSeconds() == 0)
                {
                    String fileName = Utils.fileNameFromPath(ppUserFilePath);
                    System.out.println("File: " + fileName + "has 0 residence seconds visit");
                    namesOfFilesHavingZeroResSecVisits.add(fileName);

                    break;
                } // if
            } // for

        } // for


        if(namesOfFilesHavingZeroResSecVisits.isEmpty()) System.out.println("Files in " + dataset
                + " do not have zero residence seconds visits");
    } // testZeroResidenceSeconds


    //helper method to test the order of elements of hash map and linked hash map
    private static void testOrderOfElements()
    {
        LinkedHashMap<Integer, Double> map1 = new LinkedHashMap<>();
        HashMap<Integer, Double> map2 = new HashMap<>();

        //for integer keys, hash map also holds instertion order
        for(int i = 1; i <= 40; i++)
        {
            map1.put(i, i * 1.0);
            map2.put(i, i * 1.0);
        } // for

        System.out.println("map1 => " + map1);
        System.out.println("map2 => " + map2);
    } // testOrderOfElements


    private static void testMergeProcedure()
    {
        ArrayList<Visit> visits = new ArrayList<Visit>();

        String userName = "new_abboip";
        Location location = new Location(0, 0);
        long timeDifference = 4 * 3600 * 1000;

        Date arrivalTimeT1 = new Date(10 * 1000 - timeDifference);
        Date arrivalTimeT2 = new Date(100 * 1000 - timeDifference);
        Date arrivalTimeT3 = new Date(160 * 1000 - timeDifference);
        Date arrivalTimeT4 = new Date(230 * 1000 - timeDifference);

        TimeSeriesInstant t1 = new TimeSeriesInstant(arrivalTimeT1, 50);
        TimeSeriesInstant t2 = new TimeSeriesInstant(arrivalTimeT2, 40);
        TimeSeriesInstant t3 = new TimeSeriesInstant(arrivalTimeT3, 30);
        TimeSeriesInstant t4 = new TimeSeriesInstant(arrivalTimeT4, 70);

        Visit v1 = new Visit(userName, location, t1);
        Visit v2 = new Visit(userName, location, t2);
        Visit v3 = new Visit(userName, location, t3);
        Visit v4 = new Visit(userName, location, t4);

        visits.add(v1);
        visits.add(v2);
        visits.add(v3);
        visits.add(v4);

        for(Visit v : visits) { System.out.println(v.toStringWithDate()); } System.out.println();
        for(Visit v : Preprocessing.mergeVisits(visits, 60 /*, null*/)) System.out.println(v.toStringWithDate());
    } // testMergeProcedure


    private static void testLocations()
    {
        System.out.println("DMS: " + 818198.7299 + " => Decimal: " + Utils.DMSToDecimal(818198.7299));
        System.out.println("DMS: " + -0439690.7522 + " => Decimal: " + Utils.DMSToDecimal(-0439690.7522));

        RoundingMode roundingMode = RoundingMode.FLOOR;
        //NMEA decimal-decimal degree in the format ddmm.mmmm
        double NMEADecimalValue = 5144.3855;
        System.out.println("NMEADecimal: " + NMEADecimalValue + " => " + " Decimal degree: " + Utils.NMEADecimalToDecimal(NMEADecimalValue, "#.#####", roundingMode));
        System.out.println("NMEADecimal: " + 4341.43106 + " => " + " Decimal degree: " + Utils.NMEADecimalToDecimal(4341.43106, "#.#####", roundingMode));
        System.out.println("NMEADecimal: " + -7217.43544 + " => " + " Decimal degree: " + Utils.NMEADecimalToDecimal(-7217.43544, "#.#####", roundingMode));
        System.out.println("NMEADecimal: " + 4342.15631 + " => " + " Decimal degree: " + Utils.NMEADecimalToDecimal(4342.15631));
        System.out.println("NMEADecimal: " + -7217.3524 + " => " + " Decimal degree: " + Utils.NMEADecimalToDecimal(-7217.3524, "#.#####", roundingMode));


        //round latitude and longitude
        System.out.println("Longitude: " + -122.39441 + " => " + Utils.format(-122.39441, "#.####", roundingMode));
        System.out.println("Longitude: " + -122.39449 + " => " + Utils.format(-122.39449 , "#.####", roundingMode));
        System.out.println("Longitude: " + -122.414 + " => " + Utils.format(-122.414 , "#.####", roundingMode));
        System.out.println("Latitude: " + 37.75139 + " => " + Utils.format(37.75139 , "#.####", roundingMode));
        System.out.println("Latitude: " + 37.75141 + " => " + Utils.format(37.75141 , "#.####", roundingMode));
        System.out.println("Latitude: " + 37.7514 + " => " + Utils.format(37.7514 , "#.####", roundingMode));

        Location loc1 = new Location(37.75063, -122.39336);
        Location loc2 = new Location(37.75064, -122.39335);

        System.out.println("Distance between " + loc1 + " and " + loc2 + " is " + loc1.distanceTo(loc2) + " meters");
        System.out.println("Distance between " + loc2 + " and " + loc1 + " is " + loc2.distanceTo(loc1) + " meters");

        System.out.println("\n\n");
        Location lyon = new Location(45.7597, 4.8422);
        Location paris = new Location(48.8567, 2.3508);
        System.out.println("Lyon: " + lyon.toSimpleString());
        System.out.println("Paris: " + paris.toSimpleString());
        System.out.println("Distance between Lyon and Paris in kilometers: " + lyon.distanceTo(paris) / 1000);
        System.out.println("Gaussian between Lyon and Paris: " + Utils.gaussian(lyon, paris, 10.0));


        Location locA = new Location(43.707170666666656, -72.27361783333333);
        Location locB = new Location(43.580416666666665, -72.16353550000001);
        System.out.println("Distance between locA and locB in kilometers: " + locA.distanceTo(locB) / 1000);
        System.out.println("Gaussian between locA and locB: " + Utils.gaussian(locA, locB, 10.0));

        System.out.println("Distance between loc1 and loc2 in kilometers: " + loc1.distanceTo(loc2) / 1000);
        System.out.println("Gaussian between loc1 and loc2: " + Utils.gaussian(loc1, loc2, 10.0));


        Location locC = new Location(47.6788206, -122.3271205);
        Location locD = new Location(47.6788206, -122.5271205);
        System.out.println("Distance between locC and locD in kilometers: " + locC.distanceTo(locD) / 1000);
        System.out.println("Gaussian between locC and locD: " + Utils.gaussian(locC, locD, 10.0));

        System.out.println("\n\n");
    } // testLocations


    public static void treeMultiMapTest()
    {
        TreeMultimap<Double, Integer> map = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
        map.put(15.12, 5);
        map.put(15.12000, 7);
        map.put(15.120, 3);
        map.put(16.1, 2);
        map.put(17.85, 1);
        map.put(17.85000, 6);
        System.out.println("Original map: " + map);


        System.out.println("filtered multimap: " + Multimaps.filterValues(map, e -> e < 5));
        System.out.println("Original map after filter operation: " + map);

        SetMultimap<Double, Integer> map2 = MultimapBuilder.treeKeys().treeSetValues(Ordering.natural().reversed()).build();
        map2.put(15.12, 5);
        map2.put(15.12000, 7);
        map2.put(15.120, 3);
        map2.put(16.1, 2);
        map2.put(17.85, 1);
        map2.put(17.85000, 6);
        System.out.println("map2: " + map2);
    } // treeMultiMapTest


    public static void testDates()
    {
        //today's date without day, month, year
        Date date = new Date();
        System.out.println(date + " - 45 seconds => " + Utils.subtractSeconds(date, 45));
        System.out.println(Utils.noOfSecondsBetween(new Date(994697165L * 1000), new Date(994697483L * 1000)));


        System.out.println("Today's date => " + date + "\nwithout day, month, year => "
                + Utils.toDateStringWithoutDayMonthYear(date));
        System.exit(0);

        Date date1 = new Date(0 * 1000); // 00:00, January 1st, 1970
        Date date2 = new Date(1 * 1000); // 00:01, January 1st, 1970

        System.out.println("date1: " + date1);
        System.out.println("date2: " + date2);
        System.out.println(date1 + " is after " + date2 + ": " + date1.after(date2));
        System.out.println(date1 + " is before " + date2 + ": " + date1.before(date2));


        System.out.println("\n");


        //If Daylight Saving Time is in effect at the specified date, the offset value is adjusted with the amount of daylight saving.
        //getOffset() method receives the amount of time in milliseconds to add to UTC to get local time.
        //Here new Date(1 * 1000) is created in local time, therefore we have to subtract offset value to get time in UTC
        Date date3 = new Date(1 * 1000 - TimeZone.getDefault().getOffset(0 * 1000));
        System.out.println(date3 + " is the UTC variant of " + date2);


        System.out.println("\n");


        //checking adding a seconds to a date
        Date date4 = new Date(86000 * 1000 - TimeZone.getDefault().getOffset(0 * 1000));
        int noOfSeconds = 800;
        System.out.println(Utils.addSeconds(date4, noOfSeconds) + " is " + noOfSeconds + " after " + date4);
        Date date5 = new Date(4000 * 1000 - - TimeZone.getDefault().getOffset(0 * 1000));
        System.out.println(Utils.addSeconds(date5, noOfSeconds) + " is " + noOfSeconds + " after " + date5);

        System.out.println("\n");

        //we create a new date and change its hours, minutes and seconds
        Date date6 = new Date(670971723000L); // April 7, 1991, 01:02:03
        int hours = 21;
        int minutes = 35;
        int seconds = 46;
        System.out.println("We set " + hours + " hours, " + minutes + " minutes, " + seconds + " seconds to the date: " + date6
                + " to obtain new date: " + Utils.getDate(date6, hours, minutes, seconds)) ;


        System.out.println("Unix epoch date: " + Utils.unixEpochDateWithDaySeconds(82800));

    } // testDates


    private static void testRandomSeeds()
    {
        //seeds:
        //8L, 97L, 129L, 5632L, 34234L, 496217L, 2068041L, 38714554L, 9355793L, 819108L, 74598L, 6653L,
        //505L, 78L, 3L, 24L, 305L, 7787L, 12633L, 217812L
        for(int index = 1; index <= 1000; index++)
        {
            System.out.println("#" + (index) + " => "
                    //+ Utils.randomTimeTPlusDeltaTInDaySeconds(300, 34234));
                    //+ Utils.randomTimeTPlusDeltaTInDaySeconds(300));
                    + Utils.randomTimeTPlusDeltaTInDaySeconds(5 * 60, 8L, index));
        } // for

        //System.out.println(new Random().nextLong());
    } // testRandomSeeds


} // class Testing
