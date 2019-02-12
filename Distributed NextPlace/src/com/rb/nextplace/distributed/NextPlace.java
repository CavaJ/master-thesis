package com.rb.nextplace.distributed;

import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Rufat Babayev on 2/6/2018.
 */
public class NextPlace implements Serializable
{
    //generated serial version UID
    private static final long serialVersionUID = 4694653889900876932L;

    //NextPlaceConfiguration instance to hold m, v, timeT and deltaT
    private NextPlaceConfiguration npConf;

    //constructor method
    public NextPlace(NextPlaceConfiguration npConf)
    {
        //initialize npConf
        this.npConf = npConf;
    } // NextPlace


    //getter method for npConf
    public NextPlaceConfiguration getNpConf()
    {
        return npConf;
    } // getNpConf



    //step 1 of Section 2.3 of the paper
    //the method will generate embedding space alphaGlobal from time series
    public Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> createEmbeddingSpace(ArrayList<TimeSeriesInstant> timeSeries)
    {
        //we go through indices from 0 to totalNumberOfTimeSeriesInstantsN;
        //insertion order is preserved in LinkedHashMap and we use it
        LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = new LinkedHashMap<>();
        int totalNumberOfTimeSeriesInstantsN = timeSeries.size();

        int indexOfBeta_N = 0;
        for(int nIndex = 0; nIndex < totalNumberOfTimeSeriesInstantsN; nIndex ++)
        {
            if(nIndex >= (npConf.getEmbeddingParameterM() - 1) * npConf.getDelayParameterV())
            {
                //create new embedding with index nIndex
                EmbeddingVector betta_nIndex = new EmbeddingVector();
                //add it to the treeMap with corresponding nIndex
                alphaGlobal.put(nIndex, betta_nIndex);

                //now iterate over embeddingParameterM iterations
                for (int index = 1; index <= npConf.getEmbeddingParameterM(); index ++)
                {
                    betta_nIndex.add(timeSeries.get(nIndex
                            - ((npConf.getEmbeddingParameterM() - index) * npConf.getDelayParameterV())));
                } // for

                //betta_nIndex is added to alpha global above, which is also true
                indexOfBeta_N = nIndex; // beta_N is the last indexed vector which is added to the embedding space alphaGlobal
            } // if
        } // for

        return new Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>(indexOfBeta_N, alphaGlobal);
    } // createEmbeddingSpace


    public LinkedHashMap<Integer, EmbeddingVector> epsN(double stdOfTimeSeries,
                                                        LinkedHashMap<Integer, EmbeddingVector> alphaGlobal,
                                                        int indexOfBeta_N,
                                                        EmbeddingVector beta_N)
    {
        if(alphaGlobal.size() == 0 || alphaGlobal.size() == 1)
            throw new RuntimeException("Embedding space is empty or contains only beta_N; cannot generate neighborhood");

        //epsilon is 10% of standard deviation of time series
        double epsilon = stdOfTimeSeries / 10.0;
        LinkedHashMap<Integer, EmbeddingVector> neighborhood = new LinkedHashMap<>();

        alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
        for(Integer nIndex : alphaGlobal.keySet())
        {
            double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, this.npConf);

            if(distance < epsilon || Double.compare(distance, epsilon) == 0)
            {
                EmbeddingVector beta_nIndex = alphaGlobal.get(nIndex);
                neighborhood.put(nIndex, beta_nIndex);
            } // if
        } // for
        alphaGlobal.put(indexOfBeta_N, beta_N);

        //return the generated neighborhood
        return neighborhood;
    } //epsN




    //This overloaded method will be used in MQPE calculation which is the subroutine of PE calculation
    public ArrayList<Visit> predictNextVisits(int numFutureVisitsToPredict,
                                              VisitHistory visitHistory,
                                              LinkedHashMap<Integer, EmbeddingVector> neighborhood,
                                              int indexOfBeta_N)
    {
        //numFutureVisitsToPredict cannot exceed max predictability rate of the given visit history
        if(numFutureVisitsToPredict > visitHistory.maxPossibleK(npConf))
            throw new RuntimeException("numFutureVisitsToPredict = " + numFutureVisitsToPredict
                                        + "exceeds max predictability rate of the given visit history");
        if(numFutureVisitsToPredict < 1)
            throw new RuntimeException("numFutureVisitsToPredict: " + numFutureVisitsToPredict + " cannot be smaller than 1");


        //predicted visits
        ArrayList<Visit> predictedVisits = new ArrayList<Visit>();
        //we will predict deltaL steps ahead of s_N from time series based on the value of k
        for (int deltaL = 1; deltaL <= numFutureVisitsToPredict; deltaL ++)
        {
            //it is possible to use the same numNeighbors for each deltaL = 1, 2, 3, ...
            //and it is also possible to use different numNeighbors for each deltaL through varargs
            //create a predicted visit and add it to the array list
            Visit predictedVisit = predictAVisitKStepsAhead(deltaL, visitHistory,
                                                            neighborhood,
                                                            indexOfBeta_N);
            //usage of array list guarantees that deltaL = i + 1 visit will come after deltaL = i visit
            //forall i from {1, numFutureVisitsToPredict}
            predictedVisits.add(predictedVisit);
            //System.out.println("k = " + deltaL + " => visit: " + predictedVisit.toStringWithDate());


            //if current predicted visit is null, then further visits will also be null
            //therefore return predictedVisits
            if(predictedVisit == null)
                return predictedVisits;
        } // for

        //System.exit(0);

        //returned array list should have elements sorted in ascending order or natural order
        return predictedVisits;
    } // predictNextVisits


    //overloaded method which creates and embedding space, creates neighborhood and predicts a visit
    //during the neighborhood generation alphaGlobal is refined from embedding vectors
    //which are not at least deltaL - 1 steps back from beta_N
    public Visit predictAVisitKStepsAhead(int deltaL, VisitHistory visitHistory,
                                          LinkedHashMap<Integer, EmbeddingVector> neighborhood,
                                          int indexOfBeta_N)
    {
        //deltaL cannot exceed max predictability rate of the given visit history
        if(deltaL > visitHistory.maxPossibleK(npConf))
        {
            throw new RuntimeException
                    ("Given future step K = " + deltaL
                            + "exceeds max predictability rate of the given visit history");
        } // if
        if(deltaL < 1)
        {
            throw new RuntimeException
                    ("Given future step K: " + deltaL + " cannot be smaller than 1");
        } // if



        //obtain time series
        ArrayList<TimeSeriesInstant> timeSeriesOfThisVisitHistory = visitHistory.getTimeSeries();



        //if neighborhood is empty, throw an exception
        if(neighborhood.isEmpty())
        {
            //throw new RuntimeException("Empty neighborhood, prediction is not possible");

            //System.err.println("Neighborhood is empty; returning null as a predicted visit...");
            return null;
        } // if



        //will contain instants to be averaged to s_N + deltaL
        //it will have neighborhood.size() number of elements
        ArrayList<TimeSeriesInstant> instantsToBeAveraged = new ArrayList<TimeSeriesInstant>();

        //values are in order the natural order of the keys
        //according to javadoc:
        //"The collection's iterator returns the values in ascending order of the corresponding keys."
        //which is OK.
        for(int indexOfBeta_n : neighborhood.keySet())
        {
            //EmbeddingVector beta_n = neighborhood.get(indexOfBeta_n);

            //if indexOfBeta_n can see deltaL steps ahead
            if(indexOfBeta_n < indexOfBeta_N - (deltaL - 1))
            {
                //s_n is the last element in beta_n, therefore size() - 1
                //ArrayList<TimeSeriesInstant> timeSeriesOfBetta_n = beta_n.getTimeSeries();
                //TimeSeriesInstant instant_s_n = timeSeriesOfBetta_n.get(timeSeriesOfBetta_n.size() - 1);
                //obtain the index of instant_s_n from the original non-embedded time series
                //int indexOf_instant_s_n = timeSeriesOfThisVisitHistory.indexOf(instant_s_n); // not efficient

                //now we can obtain the element at the index "indexOf_instant_s_n + deltaL"
                //index can be out of bounds, in a case of B_10 where B_9 is in neighborhood and deltaL = 3
                //but during generation of the neighborhood we only kept elements in the neighborhood
                //which is at least kStartingFrom_1_DoubledIfNecessary_And_AdjustedUpToMaxKOfHighestFrequentVisitHistory steps back s_N of B_N
                TimeSeriesInstant instant_s_n_plus_deltaL = timeSeriesOfThisVisitHistory.get(
                        //indexOf_instant_s_n
                        indexOfBeta_n // which is also the index of instant s_n
                        + deltaL);

                //add the instant s_n plus delta_L as one of the instants to be averaged
                instantsToBeAveraged.add(instant_s_n_plus_deltaL);
            } // for
        } // for


        //if instantsToBeAveraged is empty, then return null
        //because there is a possibility that no embedding vectors can satisfy indexOfBeta_n < indexOfBeta_N - (deltaL - 1)
        if(instantsToBeAveraged.isEmpty())
            return null;



        //now calculate the prediction
        TimeSeriesInstant prediction_N_plus_deltaL
                = Utils.instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(instantsToBeAveraged);
                //= Utils.instantWithAverageDate(instantsToBeAveraged);


        //add instantIndex to the prediction for convenience
        //prediction_N_plus_deltaL.setInstantIndex(indexOfBeta_N + deltaL); NO INDEX NEEDED
        //this timer series instant is actually a prediction
        prediction_N_plus_deltaL.setPrediction(true);

        //create a predicted visit and return it
        return new Visit(visitHistory.getUserName(), visitHistory.getLocation(), prediction_N_plus_deltaL);
    } // predictAVisitKStepsAhead






    //check whether the visit's
    public boolean isInInterval(Visit v_i, long timeTInDaySeconds, long deltaTSeconds, long errorMarginThetaInSeconds)
    {
        //obtain predicted time series instant
        TimeSeriesInstant p_i = v_i.getTimeSeriesInstant();

        //original t_i
        Date t_i = p_i.getArrivalTime();
        //original t_i + d_i
        Date t_i_Plus_d_i = Utils.addSeconds(t_i, p_i.getResidenceTimeInSeconds());

        //apply error margin to the left of t_i such that t_i = t_i - theta
        t_i = Utils.subtractSeconds(t_i, errorMarginThetaInSeconds);
        //apply error margin to the right of (t_i + d_i) such that (t_i + d_i) = (t_i + d_i) + theta
        t_i_Plus_d_i = Utils.addSeconds(t_i_Plus_d_i, errorMarginThetaInSeconds);

        //hat_T = (T + delta_T) mod 86400
        long hat_T = (timeTInDaySeconds + deltaTSeconds) % 86400;
        Date hat_T_t_i = Utils.getDate(t_i, hat_T);

        //if hat_T_t_i is before t_i
        if(hat_T_t_i.before(t_i))
        {
            if ( Utils.onTheSameDay(t_i, t_i_Plus_d_i) )
                return false; // do not pick v_i;
            else    //t_i and t_i + d_i are not the same day; i.e. t_i + d_i is on the next day
            {
                //obtain the version of hat_T with respect to t_i + d_i
                Date hat_T_t_i_Plus_d_i = Utils.getDate(t_i_Plus_d_i, hat_T);

                //if hat_T_t_i+d_i is before or equals t_i + d_i, then pick v_i
                if(hat_T_t_i_Plus_d_i.before(t_i_Plus_d_i) || hat_T_t_i_Plus_d_i.equals(t_i_Plus_d_i))
                    return true; // pick v_i;
                else
                    return false; //do not pick v_i; since t_i < 00:00 <= t_i + d_i < hat_T_t_i+d_i
            } // else
        } // if
        else //t_i <= hat_T_t_i
        {
            if( Utils.onTheSameDay(t_i, t_i_Plus_d_i) )
            {
                if( hat_T_t_i.before(t_i_Plus_d_i) || hat_T_t_i.equals(t_i_Plus_d_i) )
                    return true; //pick v_i
                else // t_i < t_i + d_i < hat_T_t_i < 00:00;
                    return false; // do not pick v_i
            } // if
            else
                return true; //t_i + d_i are the next day; pick v_i, since t_i and hat_T_t_i are on the previous day
        } // else


    } // isInInterval



    //helper method to be used in prediction precision calculation;
    //it is used to pick a visit a time_T + delta_T with the original way of extending predictions
    public Visit pickVisitAtTimeTPlusDeltaTOriginal(int futureStepK,
                                                    TreeSet<Visit> sequenceOfPredictedVisits,
                                                    Collection<VisitHistory> visitHistories,
                                                    HashMap<Integer, Integer> vhIndexOfBetaNMap,
                                                    HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap,
                                                    HashMap<Integer, Integer> vhLimitFutureStepKMap,
                                                    long timeTInDaySeconds,
                                                    long deltaTSeconds,
                                                    long errorMarginThetaInSeconds)
    {
        ensureCorrectnessOfTDeltaTAndErrorMargin(timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);

        //if timeT is bigger than 86400 seconds
        //perform mod 86400
        if(timeTInDaySeconds > 86400)
            timeTInDaySeconds = timeTInDaySeconds % 86400;


        //flag for extending predictions
        boolean extendPredictions = true;
        //continue until extendPredictions flag is false
        while(extendPredictions)
        {
            //future visits at timeT + deltaT can be more than one
            //therefore create array list add them there
            ArrayList<Visit> predictedVisitsAtTimeTPlusDeltaT = new ArrayList<Visit>();

            //loop through visits in chronological order
            for(Visit v : sequenceOfPredictedVisits)
            {
                //check whether v contains T+delta_T using novel isInInterval() method
                if(isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
                    predictedVisitsAtTimeTPlusDeltaT.add(v);
            } // for

            //if there is only one visit in the array list return it
            if(predictedVisitsAtTimeTPlusDeltaT.size() == 1)
                return predictedVisitsAtTimeTPlusDeltaT.get(0);
            else if(predictedVisitsAtTimeTPlusDeltaT.size() > 1)
            {
                //METHOD 1: random visit
                //return Utils.randomVisit(predictedVisitsAtTimeTPlusDeltaT); //TO BE USED BY THE SERIAL NEXTPLACE

                //METHOD 2: visit with the highest residence seconds
                return Utils.randomVisitWithHighestResidenceSeconds(predictedVisitsAtTimeTPlusDeltaT);
            } // else if
            else  //predictedVisitsAtTimeTPlusDeltaT is empty
            {
                //instants of visits in sequenceOfPredictedVisits are sorted in chronological order
                //Date part starts at Unix Epoch - 1 Jan 00:00, 1970,
                //which means that instant are sorted in the order of arrival time in day seconds
                TimeSeriesInstant firstInstant = sequenceOfPredictedVisits.first().getTimeSeriesInstant();

                //t_min which is the minimum time in instants of predicted visits,
                //and first instant is always the smallest one in terms of arrival time in day seconds;
                //d_min is the residence seconds of firstInstant which has minimum time among other instants
                Date t_min = firstInstant.getArrivalTime();
                long d_min = firstInstant.getResidenceTimeInSeconds();

                //obtain original t_min + d_min
                Date t_min_Plus_d_min = Utils.addSeconds(t_min, d_min);


                //there are three possibilities:
                //hat_T = T+delta_T;
                //hat_T_t_min is the version of hat_T with respect to t_min;
                //hat_T_t_min+d_min is the version of hat_T with respect to t_min+d_min;
                //1) t_min < t_min+d_min < hat_T_t_min < 00:00 => extend the prediction
                //2) t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min => extend the prediction
                //3) t_min < t_min + d_min < 00:00 and hat_T_t_min < t_min => do not extend
                //x) t_min < 00:00 <= t_min + d_min and hat_T < (c_min of t_min) => cannot happen because it is handled by 2) => extends the prediction

                //apply error margin to t_min
                t_min = Utils.subtractSeconds(t_min, errorMarginThetaInSeconds);
                //apply error margin to t_min+d_min
                t_min_Plus_d_min = Utils.addSeconds(t_min_Plus_d_min, errorMarginThetaInSeconds);

                //obtain hat_T
                long hat_T = (timeTInDaySeconds + deltaTSeconds) % 86400;

                //now we check all three possibilities presented above;
                //the cases where T+delta_T are contained in a visit is handled by isInInterval() method;
                if( Utils.onTheSameDay(t_min, t_min_Plus_d_min) )
                {
                    //check whether hat_T is after t_min+d_min
                    Date hat_T_t_min = Utils.getDate(t_min, hat_T);
                    if( t_min_Plus_d_min.before(hat_T_t_min) )
                        extendPredictions = true;
                    else if( hat_T_t_min.before(t_min) ) //hat_T_t_min < t_min
                        //extension is still possible, we do not extend by the definition of NextPlace
                        //assume t_min is 11:00 p.m. Jan 1, 1970, t_min+d_min is 11:30 p.m. January 1, 1970
                        //and hat_T = 3:00 a.m.
                        //=> THEN we can assume it is possible to reach 3:00 a.m. from 11:30 p.m. by extension
                        extendPredictions = false;
                    else
                        //equality case handled by isInterval function and we do not extend and return a found visit
                        extendPredictions = false;
                } // if
                else // t_min and t_min + d_min are not on the same day and
                    // this condition always holds (because of check in isInInterval) => t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min;
                    // extend the predictions;
                    extendPredictions = false; //extension is still possible, we do not extend by the definition of NextPlace


                //double the K and repeat the algorithm
                if(extendPredictions)
                {
                    //No location prediction is found at time T + delta_T for the futureStepK
                    //Extending the predictions => t_min + d_min < T + delta_T


                    //double the futureStepK to have more predictions in the future
                    //as proposed in the original NextPlace paper
                    futureStepK = futureStepK * 2;



                    //generate new sequence of predicted visits at doubled future step K
                    sequenceOfPredictedVisits = new TreeSet<>();
                    //for each visit history use precomputed alpha global, distance-index pairs and std
                    for(VisitHistory vh : visitHistories)
                    {
                        Integer hashOfVh = vh.hashCode();
                        //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVh);
                        //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
                        //        indexOfBeta_NAlphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfVh);
                        int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVh); //indexOfBeta_NAlphaGlobalTuple2._1;
                        //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_NAlphaGlobalTuple2._2;
                        //double std = vhStdMap.get(hashOfVh);
                        //int numNeighbors = vhNumNeighborsMap.get(hashOfVh);
                        LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVh);

                        //obtain maxK of this visit history
                        //int maxK = vh.maxPossibleK(npConf);

                        //if doubled futureStepK <= maxK, then it is worth predict a visit at doubled futureStepK for this vh
                        if(futureStepK <= vhLimitFutureStepKMap.get(hashOfVh)) //maxK) // maxK degrades predictive performance
                        {
                            //it is possible that for this visit history, predicted visit can be null for one reason:
                            //1) Neighborhood can be empty and no predictions occur
                            Visit predictedVisit
                                    = predictAVisitKStepsAhead(futureStepK,
                                    vh,
                                    neighborhood,
                                    indexOfBeta_N);

                            //if a visit is non-null, add it to the set of predicted visits
                            if(predictedVisit != null)
                            {
                                //update tree set with new visits
                                sequenceOfPredictedVisits.add(predictedVisit);
                            } // if
                        } // if
                    } // for



                    //check whether sequenceOfPredictedVisits is still empty;
                    //if it is empty, then further predictions will also make it empty
                    //by the nature of epsilon-neighborhood;
                    //therefore return null; by stating that user will not in any significant location
                    if(sequenceOfPredictedVisits.isEmpty())
                    {
                        //predictedVisitsAtDoubledFutureStepK is empty,
                        //further predictions will not occur, if future step K is doubled again;
                        //because all visit histories have empty epsilon neighborhood for this future step K already;
                        //returning null as a predicted location


                        return null; // null is returned here because of insufficient statistics
                        // but it means no significant location for the algorithm
                    } // if
                    //otherwise sequence of predicted visits is non-empty, so it is possible pick a visit from at T + delta_T

                } // if extendPredictions

            } // else //predictedVisitsAtTimeTPlusDeltaT is empty

        } // while extendPredictions

        //do not extend the predictions, return null noting that user will not be in any significant location
        return null;




        //REPLACED RECURSION BY ITERATION ABOVE, BUT BOTH WORKS
//        //future visits at timeT + deltaT can be more than one
//        //therefore create array list add them there
//        ArrayList<Visit> predictedVisitsAtTimeTPlusDeltaT = new ArrayList<Visit>();
//
//        //loop through visits in chronological order
//        for(Visit v : sequenceOfPredictedVisits)
//        {
//            //check whether v contains T+delta_T using novel isInInterval() method
//            if(isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
//                predictedVisitsAtTimeTPlusDeltaT.add(v);
//        } // for
//
//        //if there is only one visit in the array list return it
//        if(predictedVisitsAtTimeTPlusDeltaT.size() == 1)
//            return predictedVisitsAtTimeTPlusDeltaT.get(0);
//        else if(predictedVisitsAtTimeTPlusDeltaT.size() > 1)
//        {
//            //METHOD 1: random visit
//            //return Utils.randomVisit(predictedVisitsAtTimeTPlusDeltaT);
//
//            //METHOD 2: visit with the highest residence seconds
//            return Utils.randomVisitWithHighestResidenceSeconds(predictedVisitsAtTimeTPlusDeltaT);
//        } // else if
//
//        else    //predictedVisitsAtTimeTPlusDeltaT.size() < 1
//        {
//            //instants of visits in sequenceOfPredictedVisits are sorted in chronological order
//            //Date part starts at Unix Epoch - 1 Jan 00:00, 1970,
//            //which means that instant are sorted in the order of arrival time in day seconds
//            TimeSeriesInstant firstInstant = sequenceOfPredictedVisits.first().getTimeSeriesInstant();
//
//            //t_min which is the minimum time in instants of predicted visits,
//            //and first instant is always the smallest one in terms of arrival time in day seconds;
//            //d_min is the residence seconds of firstInstant which has minimum time among other instants
//            Date t_min = firstInstant.getArrivalTime();
//            long d_min = firstInstant.getResidenceTimeInSeconds();
//
//            //obtain original t_min + d_min
//            Date t_min_Plus_d_min = Utils.addSeconds(t_min, d_min);
//
//
//            //there are three possibilities:
//            //hat_T = T+delta_T;
//            //hat_T_t_min is the version of hat_T with respect to t_min;
//            //hat_T_t_min+d_min is the version of hat_T with respect to t_min+d_min;
//            //1) t_min < t_min+d_min < hat_T_t_min < 00:00 => extend the prediction
//            //2) t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min => extend the prediction
//            //3) t_min < t_min + d_min < 00:00 and hat_T_t_min < t_min => do not extend
//            //x) t_min < 00:00 <= t_min + d_min and hat_T < (c_min of t_min) => cannot happen because it is handled by 2) => extends the prediction
//
//            //apply error margin to t_min
//            t_min = Utils.subtractSeconds(t_min, errorMarginThetaInSeconds);
//            //apply error margin to t_min+d_min
//            t_min_Plus_d_min = Utils.addSeconds(t_min_Plus_d_min, errorMarginThetaInSeconds);
//
//            //obtain hat_T
//            long hat_T = (timeTInDaySeconds + deltaTSeconds) % 86400;
//
//            //flag which determines whether to extend the predictions
//            boolean extendPredictions;
//
//            //now we check all three possibilities presented above;
//            //the cases where T+delta_T are contained in a visit is handled by isInInterval() method;
//            if( Utils.onTheSameDay(t_min, t_min_Plus_d_min) )
//            {
//                //check whether hat_T is after t_min+d_min
//                Date hat_T_t_min = Utils.getDate(t_min, hat_T);
//                if( t_min_Plus_d_min.before(hat_T_t_min) )
//                    extendPredictions = true;
//                else if( hat_T_t_min.before(t_min) ) //hat_T_t_min < t_min
//                    //extension is still possible, we do not extend by the definition of NextPlace
//                    //assume t_min is 11:00 p.m. Jan 1, 1970, t_min+d_min is 11:30 p.m. January 1, 1970
//                    //and hat_T = 3:00 a.m.
//                    //=> THEN we can assume it is possible to reach 3:00 a.m. from 11:30 p.m. by extension
//                    extendPredictions = false;
//                else
//                    //equality case handled by isInterval function and we do not extend and return a found visit
//                    extendPredictions = false;
//            } // if
//            else // t_min and t_min + d_min are not on the same day and
//                // this condition always holds (because of check in isInInterval) => t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min;
//                // extend the predictions;
//                extendPredictions = false; //extension is still possible, we do not extend by the definition of NextPlace
//
//
//            //double the K and repeat the algorithm
//            if(extendPredictions)
//            {
//                //No location prediction is found at time T + delta_T for the futureStepK
//                //Extending the predictions => t_min + d_min < T + delta_T
//
//
//                //doubled futureStepK to have more predictions in the future
//                //as proposed in the original NextPlace paper
//                int doubledFutureStepK = futureStepK * 2;
//
//
//                TreeSet<Visit> predictedVisitsAtDoubledFutureStepK = new TreeSet<>();
//                //for each visit history use precomputed alpha global, distance-index pairs, std
//                //or possibly optimal numNeighbors K
//                for(VisitHistory vh : visitHistories)
//                {
//                    Integer hashOfVh = vh.hashCode();
//                    //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVh);
//                    //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
//                    //        indexOfBeta_NAlphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfVh);
//                    int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVh); //indexOfBeta_NAlphaGlobalTuple2._1;
//                    //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_NAlphaGlobalTuple2._2;
//                    //double std = vhStdMap.get(hashOfVh);
//                    //int numNeighbors = vhNumNeighborsMap.get(hashOfVh);
//                    LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVh);
//
//                    //obtain maxK of this visit history
//                    //int maxK = vh.maxPossibleK(npConf);
//
//                    //if doubledFutureStepK <= maxK, then it is worth predict a visit at doubledFutureStepK for this vh
//                    if(doubledFutureStepK <= vhLimitFutureStepKMap.get(hashOfVh)) //maxK) // maxK degrades predictive performance
//                    {
//                        //it is possible that for this visit history, predicted visit can be null for one reason:
//                        //1) Neighborhood can be empty and no predictions occur
//                        Visit predictedVisitAtDoubledFutureStepK
//                                = predictAVisitKStepsAhead(doubledFutureStepK,
//                                        vh,
//                                        //std,
//                                        //alphaGlobal,
//                                        neighborhood,
//                                        indexOfBeta_N);
//                                        //distanceIndexPairs); //, numNeighbors);
//
//                        //if a visit is non-null, add it to the set of predicted visits
//                        if(predictedVisitAtDoubledFutureStepK != null)
//                        {
//                            //update tree set with new visits
//                            predictedVisitsAtDoubledFutureStepK.add(predictedVisitAtDoubledFutureStepK);
//                        } // if
//                    } // if
//                } // for
//
//
//
//                //check whether predictedVisitsAtDoubledFutureStepK is still empty;
//                //if it is empty, then further predictions will also make it empty
//                //by the nature of epsilon-neighborhood;
//                //therefore return null; by stating that user will not in any significant location
//                if(predictedVisitsAtDoubledFutureStepK.isEmpty())
//                {
//                    //predictedVisitsAtDoubledFutureStepK is empty,
//                    //further predictions will not occur, if future step K is doubled again;
//                    //because all visit histories have empty epsilon neighborhood for this future step K already;
//                    //returning null as a predicted location
//
//                    return null; // null is returned here because of insufficient statistics
//                    // but it means no significant location for the algorithm
//                } // if
//                else
//                    //pick a new visit from predictedVisitsAtDoubledFutureStepK and return it
//                    return pickVisitAtTimeTPlusDeltaTOriginal(doubledFutureStepK,
//                            predictedVisitsAtDoubledFutureStepK, visitHistories,
//                            vhIndexOfBetaNMap,
//                            vhNeighborhoodMap,
//                            vhLimitFutureStepKMap,
//                            timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);
//            } // if
//            else    // do not extend the predictions
//            {
//                //Not extended the predictions further => T + delta_T < t_min
//                //The user will not be in any significant locations
//
//                //user will not be in any significant locations, return null
//                return null;
//            } // else
//
//        } // else //predictedVisitsAtTimeTPlusDeltaT.size() < 1


    } // pickVisitAtTimeTPlusDeltaTOriginal





    //helper method to pick a visit a time_T + delta_T for the original way of extending predictions
    public Visit pickVisitAtTimeTPlusDeltaTOriginal(StringBuilder globalResultStringBuilder,
                                                    int futureStepK,
                                                    TreeSet<Visit> sequenceOfPredictedVisits,
                                                    Collection<VisitHistory> visitHistories,
                                                    HashMap<Integer, Integer> vhIndexOfBetaNMap,
                                                    HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap,
                                                    HashMap<Integer, Integer> vhLimitFutureStepKMap,
                                                    long timeTInDaySeconds,
                                                    long deltaTSeconds,
                                                    long errorMarginThetaInSeconds)
    {
        ensureCorrectnessOfTDeltaTAndErrorMargin(timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);

        //if timeT is bigger than 86400 seconds
        //perform mod 86400
        if(timeTInDaySeconds > 86400)
            timeTInDaySeconds = timeTInDaySeconds % 86400;


        //future visits at timeT + deltaT can be more than one
        //therefore create array list add them there
        ArrayList<Visit> predictedVisitsAtTimeTPlusDeltaT = new ArrayList<Visit>();

        //loop through visits in chronological order
        for(Visit v : sequenceOfPredictedVisits)
        {
            //check whether v contains T+delta_T using novel isInInterval() method
            if(isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
                predictedVisitsAtTimeTPlusDeltaT.add(v);
        } // for

        //if there is only one visit in the array list return it
        if(predictedVisitsAtTimeTPlusDeltaT.size() == 1)
            return predictedVisitsAtTimeTPlusDeltaT.get(0);
        else if(predictedVisitsAtTimeTPlusDeltaT.size() > 1)
        {
            //METHOD 1: random visit
            //return Utils.randomVisit(predictedVisitsAtTimeTPlusDeltaT); // TO BE USED BY THE SERIAL NEXTPLACE

            //METHOD 2: visit with the highest residence seconds
            return Utils.randomVisitWithHighestResidenceSeconds(predictedVisitsAtTimeTPlusDeltaT);
        } // else if

        else    //predictedVisitsAtTimeTPlusDeltaT.size() < 1
        {
            //instants of visits in sequenceOfPredictedVisits are sorted in chronological order
            //Date part starts at Unix Epoch - 1 Jan 00:00, 1970,
            //which means that instant are sorted in the order of arrival time in day seconds
            TimeSeriesInstant firstInstant = sequenceOfPredictedVisits.first().getTimeSeriesInstant();

            //t_min which is the minimum time in instants of predicted visits,
            //and first instant is always the smallest one in terms of arrival time in day seconds;
            //d_min is the residence seconds of firstInstant which has minimum time among other instants
            Date t_min = firstInstant.getArrivalTime();
            long d_min = firstInstant.getResidenceTimeInSeconds();

            //obtain original t_min + d_min
            Date t_min_Plus_d_min = Utils.addSeconds(t_min, d_min);


            //there are three possibilities:
            //hat_T = T+delta_T;
            //hat_T_t_min is the version of hat_T with respect to t_min;
            //hat_T_t_min+d_min is the version of hat_T with respect to t_min+d_min;
            //1) t_min < t_min+d_min < hat_T_t_min < 00:00 => extend the prediction
            //2) t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min => extend the prediction
            //3) t_min < t_min + d_min < 00:00 and hat_T_t_min < t_min => do not extend
            //x) t_min < 00:00 <= t_min + d_min and hat_T < (c_min of t_min) => cannot happen because it is handled by 2) => extends the prediction

            //apply error margin to t_min
            t_min = Utils.subtractSeconds(t_min, errorMarginThetaInSeconds);
            //apply error margin to t_min+d_min
            t_min_Plus_d_min = Utils.addSeconds(t_min_Plus_d_min, errorMarginThetaInSeconds);

            //obtain hat_T
            long hat_T = (timeTInDaySeconds + deltaTSeconds) % 86400;

            //flag which determines whether to extend the predictions
            boolean extendPredictions;

            //now we check all three possibilities presented above;
            //the cases where T+delta_T are contained in a visit is handled by isInInterval() method;
            if( Utils.onTheSameDay(t_min, t_min_Plus_d_min) )
            {
                //check whether hat_T is after t_min+d_min
                Date hat_T_t_min = Utils.getDate(t_min, hat_T);
                if( t_min_Plus_d_min.before(hat_T_t_min) )
                    extendPredictions = true;
                else if( hat_T_t_min.before(t_min) ) //hat_T_t_min < t_min
                    //extension is still possible, we do not extend by the definition of NextPlace
                    //assume t_min is 11:00 p.m. Jan 1, 1970, t_min+d_min is 11:30 p.m. January 1, 1970
                    //and hat_T = 3:00 a.m.
                    //=> THEN we can assume it is possible to reach 3:00 a.m. from 11:30 p.m. by extension
                    extendPredictions = false;
                else
                    //equality case handled by isInterval function and we do not extend and return a found visit
                    extendPredictions = false;
            } // if
            else // t_min and t_min + d_min are not on the same day and
                // this condition always holds (because of check in isInInterval) => t_min < 00:00 <= t_min + d_min < hat_T_t_min+d_min;
                // extend the predictions;
                extendPredictions = false; //extension is still possible, we do not extend by the definition of NextPlace


            //double the K and repeat the algorithm
            if(extendPredictions)
            {
                globalResultStringBuilder.append("No location prediction is found at time T + delta_T = ")
                        .append(hat_T).append(" for the future step ")
                        .append(futureStepK).append("\n");
                globalResultStringBuilder.append("Extending the predictions => t_min + d_min < T + delta_T: ")
                        .append(Utils.toDaySeconds(t_min_Plus_d_min)).append(" < ").append(hat_T).append("\n");

                //doubled futureStepK to have more predictions in the future
                //as proposed in the original NextPlace paper
                int doubledFutureStepK = futureStepK * 2;


                TreeSet<Visit> predictedVisitsAtDoubledFutureStepK = new TreeSet<>();
                //for each visit history use precomputed alpha global, distance-index pairs, std
                //or possibly optimal numNeighbors K
                for(VisitHistory vh : visitHistories)
                {
                    Integer hashOfVh = vh.hashCode();
                    //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVh);
                    //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
                    //        indexOfBeta_NAlphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfVh);
                    int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVh); //indexOfBeta_NAlphaGlobalTuple2._1;
                    //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_NAlphaGlobalTuple2._2;
                    //double std = vhStdMap.get(hashOfVh);
                    //int numNeighbors = vhNumNeighborsMap.get(hashOfVh);
                    LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVh);

                    //obtain maxK of this visit history
                    //int maxK = vh.maxPossibleK(npConf);

                    //if doubledFutureStepK <= maxK, then it is worth predict a visit at doubledFutureStepK for this vh
                    if(doubledFutureStepK <= vhLimitFutureStepKMap.get(hashOfVh)) //maxK) // maxK degrades predictive performance
                    {
                        //it is possible that for this visit history, predicted visit can be null for one reason:
                        //1) Neighborhood can be empty and no predictions occur
                        Visit predictedVisitAtDoubledFutureStepK
                                = predictAVisitKStepsAhead(doubledFutureStepK,
                                vh,
                                neighborhood,
                                indexOfBeta_N);

                        //if a visit is non-null, add it to the set of predicted visits
                        if(predictedVisitAtDoubledFutureStepK != null)
                        {
                            //update tree set with new visits
                            predictedVisitsAtDoubledFutureStepK.add(predictedVisitAtDoubledFutureStepK);
                        } // if
                    } // if
                } // for




                globalResultStringBuilder.append("\n======= futureStepK is increased from ")
                        .append(futureStepK).append(" to ").append(doubledFutureStepK)
                        .append("; New predicted visits ").append("in chronological order for deltaL = ")
                        .append(doubledFutureStepK).append("; #Predicted Visits: ")
                        .append(predictedVisitsAtDoubledFutureStepK.size()).append("  =======\n");
                for (Visit v : predictedVisitsAtDoubledFutureStepK)
                    globalResultStringBuilder.append(v.toStringWithDate()).append("\n");
                //make one more space
                globalResultStringBuilder.append("\n");



                //check whether predictedVisitsAtDoubledFutureStepK is still empty;
                //if it is empty, then further predictions will also make it empty
                //by the nature of epsilon-neighborhood;
                //therefore return null; by stating that user will not in any significant location
                if(predictedVisitsAtDoubledFutureStepK.isEmpty())
                {
                    globalResultStringBuilder.append("predictedVisitsAtDoubledFutureStepK is empty, ")
                            .append("further predictions will not occur, if future step K is doubled again; ")
                            .append("because all visit histories have empty epsilon neighborhood for this future step K already; ")
                            .append("returning null as a predicted location.")
                            .append("\n");

                    return null; // null is returned here because of insufficient statistics
                                // but it means no significant location for the algorithm
                } // if
                else
                    //pick a new visit from predictedVisitsAtDoubledFutureStepK and return it
                    return pickVisitAtTimeTPlusDeltaTOriginal(globalResultStringBuilder, doubledFutureStepK,
                            predictedVisitsAtDoubledFutureStepK, visitHistories,
                            vhIndexOfBetaNMap,
                            vhNeighborhoodMap,
                            vhLimitFutureStepKMap,
                            errorMarginThetaInSeconds, timeTInDaySeconds, deltaTSeconds);
            } // if
            else    // do not extend the predictions
            {
                globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");
                globalResultStringBuilder.append("Not extended the predictions further => T + delta_T < t_min: ")
                        .append(hat_T).append(" < ").append(Utils.toDaySeconds(t_min)).append("\n");
                globalResultStringBuilder.append("The user will not be in any significant locations; ")
                        .append("returning null as a predicted location.")
                        .append("\n");
                globalResultStringBuilder.append("\n-----------------------------------------------------------------------\n");

                //user will not be in any significant locations
                return null;
            } // else


        } // else //predictedVisitsAtTimeTPlusDeltaT.size() < 1
    } // pickVisitAtTimeTPlusDeltaTOriginal



    private void ensureCorrectnessOfTDeltaTAndErrorMargin(long timeTInDaySeconds,
                                                          long deltaTSeconds,
                                                          long errorMarginThetaInSeconds)
    {
        if(timeTInDaySeconds < 0)
            throw new RuntimeException("Time T cannot be negative");
        if(deltaTSeconds < 0)
            throw new RuntimeException("Delta T cannot be negative");
        if(errorMarginThetaInSeconds < 0)
            throw new RuntimeException("Error margin Theta cannot be negative");
    } // ensureCorrectnessOfTDeltaTAndErrorMargin



    //used in prediction precision calculation
    public Visit pickVisitAtTimeTPlusDeltaTUpToPredLimit(int futureStepK,
                                                         int limitOnFutureStepK, // can be maxKOfHighestFrequentVisitHistory
                                                         TreeSet<Visit> sequenceOfPredictedVisits,
                                                         Collection<VisitHistory> visitHistories,
                                                         HashMap<Integer, Integer> vhIndexOfBetaNMap,
                                                         HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap,
                                                         HashMap<Integer, Integer> vhLimitFutureStepKMap,
                                                         long timeTInDaySeconds,
                                                         long deltaTSeconds,
                                                         long errorMarginThetaInSeconds)
    {
        ensureCorrectnessOfTDeltaTAndErrorMargin(timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);

        //if timeT is bigger than 86400 seconds
        //perform mod 86400
        if(timeTInDaySeconds > 86400)
            timeTInDaySeconds = timeTInDaySeconds % 86400;


        //while will stop when limit is reached; futureStepK > limitOnFutureStepK;
        //if limitOnFutureStepK is maxK', the all statistical potential in visit histories will be used
        while(futureStepK < limitOnFutureStepK + 1) // add 1 to make sure that at step limitOnFutureStepK, new visit are checked
        {
            //future visits at timeT + deltaT can be more than one
            //therefore create array list add them there
            ArrayList<Visit> predictedVisitsAtTimeTPlusDeltaT = new ArrayList<Visit>();


            //loop through visits in chronological order
            for(Visit v : sequenceOfPredictedVisits)
            {
                //check whether v contains T+delta_T using novel isInInterval() method
                if(isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
                    predictedVisitsAtTimeTPlusDeltaT.add(v);
            } // for

            //if there is only one visit in the array list return it
            if(predictedVisitsAtTimeTPlusDeltaT.size() == 1)
                return predictedVisitsAtTimeTPlusDeltaT.get(0);
            else if(predictedVisitsAtTimeTPlusDeltaT.size() > 1)
            {
                //METHOD 1: random visit
                //return Utils.randomVisit(predictedVisitsAtTimeTPlusDeltaT); //TO BE USED BY THE ORIGINAL NEXTPLACE

                //METHOD 2: visit with the highest residence seconds
                return Utils.randomVisitWithHighestResidenceSeconds(predictedVisitsAtTimeTPlusDeltaT);
            } // else if
            else            // predictedVisitsAtTimeTPlusDeltaT is empty
            {
                //increment futureStepK to have more predictions in the future
                futureStepK *= 2; //++;

                //if futureStepK exceeds limitOnFutureStepK,
                //this can happen for prediction after limitOnFutureStepK;
                //then return null;
                if(futureStepK > limitOnFutureStepK) return null; // for exceeding case, below for loop will also return null
                                                                  // however a check here will do it earlier


                //obtain a new sequence of predicted visits
                sequenceOfPredictedVisits = new TreeSet<>();
                //for each visit history use precomputed alpha global, distance-index pairs, std
                //or possibly optimal numNeighbors K
                for (VisitHistory vh : visitHistories)
                {
                    Integer hashOfVh = vh.hashCode();
                    //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVh);
                    //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
                    //        indexOfBeta_NAlphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfVh);
                    int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVh); //indexOfBeta_NAlphaGlobalTuple2._1;
                    //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_NAlphaGlobalTuple2._2;
                    //double std = vhStdMap.get(hashOfVh);
                    //int numNeighbors = vhNumNeighborsMap.get(hashOfVh);
                    LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVh);

                    //obtain maxK of this visit history
                    //int maxK = vh.maxPossibleK(npConf); // with maxK, it requires more time

                    //if incremented futureStepK <= maxK, then it is worth predict a visit at incremented futureStepK for this vh
                    if (futureStepK <= vhLimitFutureStepKMap.get(hashOfVh)) //maxK) // maxK can degrade performance
                    {
                        //it is possible that for this visit history, predicted visit can be null for one reason:
                        //1) Neighborhood can be empty and no predictions occur
                        Visit predictedVisit
                                = predictAVisitKStepsAhead(futureStepK,
                                vh,
                                neighborhood,
                                indexOfBeta_N);

                        //if a visit is non-null, add it to the set of predicted visits
                        if (predictedVisit != null)
                        {
                            //update tree set with new visits
                            sequenceOfPredictedVisits.add(predictedVisit);
                        } // if
                    } // if
                } // for


                //check whether sequenceOfPredictedVisits is still empty;
                //if it is empty, then further predictions will also make it empty
                //by the nature of epsilon-neighborhood;
                //therefore return null; by stating that user will not in any significant location
                if (sequenceOfPredictedVisits.isEmpty())
                {
                    //sequenceOfPredictedVisits is empty, further predictions will not occur, if future step K is incremented again; " +
                    //because all visit histories have empty epsilon neighborhood for this future step K already;
                    //return null as a predicted location

                    return null; // null is returned here because of insufficient statistics found by epsilon-neighborhoods
                    // but it means no significant location for the algorithm
                } // if
                //otherwise new iteration of while loop will be performed with non-empty sequenceOfPredictedVisits

            } // else
        } // while

        //if while loop does not return anything, then return null
        return null;

    } // pickVisitAtTimeTPlusDeltaTUpToPredLimit


    //helper method to pick a visit a time_T + delta_T for the distributed next place algorithm
    public Visit pickVisitAtTimeTPlusDeltaTUpToPredLimit(StringBuilder globalResultStringBuilder,
                                                         int futureStepK,
                                                         int limitOnFutureStepK, // can be maxKOfHighestFrequentVisitHistory
                                                         TreeSet<Visit> sequenceOfPredictedVisits,
                                                         Collection<VisitHistory> visitHistories,
                                                         HashMap<Integer, Integer> vhIndexOfBetaNMap,
                                                         HashMap<Integer, LinkedHashMap<Integer, EmbeddingVector>> vhNeighborhoodMap,
                                                         HashMap<Integer, Integer> vhLimitFutureStepKMap,
                                                         long timeTInDaySeconds,
                                                         long deltaTSeconds,
                                                         long errorMarginThetaInSeconds)
    {
        ensureCorrectnessOfTDeltaTAndErrorMargin(timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds);

        //if timeT is bigger than 86400 seconds
        //perform mod 86400
        if(timeTInDaySeconds > 86400)
            timeTInDaySeconds = timeTInDaySeconds % 86400;


        //while will stop when limit is reached; futureStepK > limitOnFutureStepK;
        //if limitOnFutureStepK is maxK', the all statistical potential in visit histories will be used
        while(futureStepK < limitOnFutureStepK + 1) // add 1 to make sure that at step limitOnFutureStepK, new visit are checked
        {

            //future visits at timeT + deltaT can be more than one
            //therefore create array list add them there
            ArrayList<Visit> predictedVisitsAtTimeTPlusDeltaT = new ArrayList<Visit>();


            //loop through visits in chronological order
            for (Visit v : sequenceOfPredictedVisits) {
                //check whether v contains T+delta_T using novel isInInterval() method
                if (isInInterval(v, timeTInDaySeconds, deltaTSeconds, errorMarginThetaInSeconds))
                    predictedVisitsAtTimeTPlusDeltaT.add(v);
            } // for


            //if there is only one visit in the array list return it
            if (predictedVisitsAtTimeTPlusDeltaT.size() == 1)
                return predictedVisitsAtTimeTPlusDeltaT.get(0);
            else if (predictedVisitsAtTimeTPlusDeltaT.size() > 1) {
                //METHOD 1: random visit
                //return Utils.randomVisit(predictedVisitsAtTimeTPlusDeltaT); //TO BE USED BY THE ORIGINAL NEXTPLACE

                //METHOD 2: visit with the highest residence seconds
                return Utils.randomVisitWithHighestResidenceSeconds(predictedVisitsAtTimeTPlusDeltaT);
            } // else if

            else    //predictedVisitsAtTimeTPlusDeltaT.size() < 1
            {
                //increment futureStepK to have more predictions in the future
                futureStepK *= 2; //++;

                //if futureStepK exceeds limitOnFutureStepK,
                //this can happen for prediction after limitOnFutureStepK;
                //then return null;
                //if(futureStepK > limitOnFutureStepK) return null; // for exceeding case, below for loop will also return null
                // however a check here will do it earlier


                globalResultStringBuilder.append("No location prediction is found at time T + delta_T = ")
                        .append(timeTInDaySeconds + deltaTSeconds).append(" for the future step ")
                        .append(futureStepK).append("; extending predictions...").append("\n");



                //obtain a new sequence of predicted visits
                sequenceOfPredictedVisits = new TreeSet<>();
                //for each visit history use precomputed alpha global, distance-index pairs, std
                //or possibly optimal numNeighbors K
                for (VisitHistory vh : visitHistories)
                {
                    Integer hashOfVh = vh.hashCode();
                    //SetMultimap<Double, Integer> distanceIndexPairs = vhDistIndexPairsMap.get(hashOfVh);
                    //Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>>
                    //        indexOfBeta_NAlphaGlobalTuple2 = vhAlphaGlobalMap.get(hashOfVh);
                    int indexOfBeta_N = vhIndexOfBetaNMap.get(hashOfVh); //indexOfBeta_NAlphaGlobalTuple2._1;
                    //LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_NAlphaGlobalTuple2._2;
                    //double std = vhStdMap.get(hashOfVh);
                    //int numNeighbors = vhNumNeighborsMap.get(hashOfVh);
                    LinkedHashMap<Integer, EmbeddingVector> neighborhood = vhNeighborhoodMap.get(hashOfVh);

                    //obtain maxK of this visit history
                    //int maxK = vh.maxPossibleK(npConf); // with maxK, it requires more time

                    //if incremented futureStepK <= maxK, then it is worth predict a visit at incremented futureStepK for this vh
                    if (futureStepK <= vhLimitFutureStepKMap.get(hashOfVh)) //maxK) // maxK can degrade performance
                    {
                        //it is possible that for this visit history, predicted visit can be null for one reason:
                        //1) Neighborhood can be empty and no predictions occur
                        Visit predictedVisit
                                = predictAVisitKStepsAhead(futureStepK,
                                vh,
                                neighborhood,
                                indexOfBeta_N);

                        //if a visit is non-null, add it to the set of predicted visits
                        if (predictedVisit != null)
                        {
                            //update tree set with new visits
                            sequenceOfPredictedVisits.add(predictedVisit);
                        } // if
                    } // if
                } // for


                globalResultStringBuilder.append("\n======= futureStepK is increased from ")
                        .append(futureStepK / 2).append(" to ").append(futureStepK)
                        .append("; New predicted visits ").append("in chronological order for deltaL = ")
                        .append(futureStepK).append("; #Predicted Visits: ")
                        .append(sequenceOfPredictedVisits.size()).append("  =======\n");
                for (Visit v : sequenceOfPredictedVisits)
                    globalResultStringBuilder.append(v.toStringWithDate()).append("\n");
                //make one more space
                globalResultStringBuilder.append("\n");


                //check whether predictedVisitsAtDoubledFutureStepK is still empty;
                //if it is empty, then further predictions will also make it empty
                //by the nature of epsilon-neighborhood;
                //therefore return null; by stating that user will not in any significant location
                if (sequenceOfPredictedVisits.isEmpty())
                {
                    globalResultStringBuilder.append("predictedVisitsAtDoubledFutureStepK is empty, ")
                            .append("further predictions will not occur, if future step K is doubled again; ")
                            .append("because all visit histories have empty epsilon neighborhood for this future step K already; ")
                            .append("return null as a predicted location.")
                            .append("\n");

                    return null; // null is returned here because of insufficient statistics found by epsilon-neighborhoods
                    // but it means no significant location for the algorithm
                } // if

            } // else //predictedVisitsAtTimeTPlusDeltaT.size() < 1

        } // while


        globalResultStringBuilder.append("Extended predictions up to the predictability limit (e.g. maxK of the highest frequent visit history) ")
                .append("; return null as a predicted location.").append("\n");

        return null;
    } // pickVisitAtTimeTPlusDeltaTUpToPredLimit




    //method to calculate "mean quadratic prediction error" or MQPE of the time series which is sorted
    //chronologically by default;
    //the real time series will be compared against predicted time series
    public double MQPE(ArrayList<TimeSeriesInstant> timeSeriesReal, ArrayList<TimeSeriesInstant> timeSeriesPredicted)
    {
        if(timeSeriesReal == null
                || timeSeriesReal.isEmpty()
                || timeSeriesPredicted == null
                || timeSeriesPredicted.isEmpty() )
        {
            throw new IllegalArgumentException("timesSeriesReal or timeSeriesPredicted is null or empty;"
                    + " no possibility to calculate MQPE - returning 0");
        } // if


        //the size of real time series and predictions can differ
        //number of comparisons define how many elements will be compared from both time series
        //we assume real and predicted have the same size and adjust below if they do not have
        //these cases can happen based on the value of embeddingParameterM, since we take delayParameterV = 1
        //real is bigger than predicted then #comparisons will be size of predicted otherwise the size of the real (including if they are equal)
        int numberOfComparisons = timeSeriesReal.size() > timeSeriesPredicted.size() ? timeSeriesPredicted.size() : timeSeriesReal.size();

        //System.out.println("number of comparisons: " + numberOfComparisons);

        //now calculate distances between real measurements and predictions and average them
        long sumOfSquaredDistances = 0;

        //aggregate distances, then average by number of comparisons
        for(int index = 0; index < numberOfComparisons; index ++)
        {
            //(s_n - p_n)
            long distanceInSeconds = timeSeriesReal.get(index).distanceInSecondsTo(timeSeriesPredicted.get(index));

            //sum += (s_n - p_n) ^ 2
            sumOfSquaredDistances += (distanceInSeconds * distanceInSeconds);
        } // for


        //denote average by epsilon_l, meaning the mean prediction error of time series at location l
        //if both sumOfSquaredDistances is 0 (which happens when numberOfComparisons is 0), we can get NaN
        //but to make this not to happen, we check whether at least one neighbor in the embedding space
        //of firstHalf in peOfVisitHistory() method
        double epsilon_l = sumOfSquaredDistances / (1.0 * numberOfComparisons);

        //return MQPE
        return epsilon_l;
    } // MQPE



    //overloaded version of MQPE to find mean quadratic prediction between two time series instants
    public double MQPE(TimeSeriesInstant real, TimeSeriesInstant predicted)
    {
        if(real == null
                || predicted == null)
        {
            throw new IllegalArgumentException("Real or predicted is null or empty;"
                    + " no possibility to calculate MQPE - returning 0");
        } // if


        //(s_n - p_n)
        long distanceInSeconds = real.distanceInSecondsTo(predicted);

        //there is only one comparison and sum contains only one squareDistance
        return distanceInSeconds * distanceInSeconds;
    } // MQPE




    //DOES NOT GIVE GOOD RESULTS
    //method to calculate "mean absolute prediction error" or MAPE of the time series which is sorted
    //chronologically by default;
    //the real time series will be compared against predicted time series
    private double MAPE(ArrayList<TimeSeriesInstant> timeSeriesReal, ArrayList<TimeSeriesInstant> timeSeriesPredicted)
    {
        if(timeSeriesReal == null
                || timeSeriesReal.isEmpty()
                || timeSeriesPredicted == null
                || timeSeriesPredicted.isEmpty() )
        {
            System.err.println("timesSeriesReal or timeSeriesPredicted is null or empty;"
                    + " no possibility to calculate MAPE - returning 0");
            return 0;
        } // if

        //the size of real time series and predictions can differ
        //number of comparisons define how many elements will be compared from both time series
        //we assume real and predicted have the same size and adjust below if they do not have
        //these cases can happen based on the value of embeddingParameterM, since we take delayParameterV = 1
        //real is bigger than predicted then #comparisons will be size of predicted otherwise the size of the real (including if they are equal)
        int numberOfComparisons = timeSeriesReal.size() > timeSeriesPredicted.size() ? timeSeriesPredicted.size() : timeSeriesReal.size();

        //System.out.println("number of comparisons: " + numberOfComparisons);

        //now calculate distances between real measurements and predictions and average them
        long sumOfDistances = 0;

        //aggregate distances, then average by number of comparisons
        for(int index = 0; index < numberOfComparisons; index ++)
        {
            //since it is an MAPE calculation other distance metric cannot be used e.g. Manhattan distance.
            //(s_n - p_n)
            long distanceInSeconds = timeSeriesReal.get(index).distanceInSecondsTo(timeSeriesPredicted.get(index));

            //sum += |(s_n - p_n)|
            sumOfDistances += distanceInSeconds;
        } // for


        //denote average by epsilon_l, meaning the mean prediction error of time series at location l
        //if both sumOfDistances is 0 (which happens when numberOfComparisons is 0), we can get NaN
        //but to make this not to happen, we check whether at least one neighbor in the embedding space
        //of firstHalf in peOfVisitHistory() method below
        double epsilon_l = sumOfDistances / (1.0 * numberOfComparisons);

        //return MQPE
        return epsilon_l;
    } // MAPE




    //DOES NOT GIVE GOOD RESULTS
    //calculates \hat{s} which a mean of the first part visit history and
    //subtracts the second part visit history elements from it.
    //in this case, \hat{s} is like simple average value prediction
    //and the result is compared against the MQPE in peOfVisitHistory method
    public double varianceWithRespectToMeanOfFirstPart(ArrayList<TimeSeriesInstant> firstPartTimeSeries,
                                                       ArrayList<TimeSeriesInstant> secondPartTimeSeries,
                                                       ArrayList<TimeSeriesInstant> predictedSecondPart)
    {
        if(secondPartTimeSeries == null
                || secondPartTimeSeries.isEmpty()
                || predictedSecondPart == null
                || predictedSecondPart.isEmpty() )
        {
            throw new IllegalArgumentException("secondPartTimeSeries or predictedSecondPart is null or empty;"
                    + " no possibility to calculate MQPE - returning 0");
        } // if

        //number of comparison to find variance
        int numberOfComparisons = secondPartTimeSeries.size() > predictedSecondPart.size() ? predictedSecondPart.size() : secondPartTimeSeries.size();

        //mean instant of the first part
        TimeSeriesInstant hat_s = Utils.instantWithAvgArrivalTimeInDaySecondsAndResidenceSeconds(firstPartTimeSeries);

        double sumOfSquaredDistances = 0;
        //now calculate the variance with respect to hat_s
        for(int index = 0; index < numberOfComparisons; index ++)
        {
            //(s_n - hat_s)
            long distanceInSeconds = secondPartTimeSeries.get(index).distanceInSecondsTo(hat_s);

            //sum += (s_n - hat_s) ^ 2
            sumOfSquaredDistances += distanceInSeconds;
        } // for


        //return variance with respect to hat_s
        return sumOfSquaredDistances / numberOfComparisons;
    } // varianceWithRespectToMeanOfFirstPart




    //method to calculate predictability error of the time series
    //which is the division of MQPE value "epsilon" and variance of the time series
    //The absolute error value of MQPE value "epsilon" may be meaningless
    //if not compared to the average amount of fluctuations a time series exhibits;
    //by dividing by the variance of the series we can normalize the error
    //and compare the prediction accuracy for different time series;
    //Since the predictability error is normalized version of MQPE
    //then it will be around 0 and 1.
    public double peOfVisitHistory(VisitHistory visitHistory, double trainSplitFraction, double testSplitFraction)
    {
        ArrayList<TimeSeriesInstant> originalTimeSeries = visitHistory.getTimeSeries();

        //length of the time series
        int originalTimeSeriesLength = originalTimeSeries.size();

        //to calculate MQPE we should compare predictions with original values;
        //for this purpose we divide given time series into two halves;
        //to make the division possible, the size of embedding space that can be created
        //from timeSeries should be bigger than or equals to 2;
        //but in general, for division to two halves, original time series size should be at least 3
        //as we know total number of embedding vectors in the embedding space of the
        //time series is timeSeries.size() - (m - 1) * v;
        //if that value is smaller than 2, the we will return 1 which is the maximal value of normalized error
        //which in turn means time series are not predictable because of insufficient statistics;
        //therefore, we do the following check make sure that at least one neighbor can be generated on original time series;
        //if no neighbors can be generated on the original time series, then for the half of it, we can't do either
        if(originalTimeSeriesLength < 3
           || originalTimeSeriesLength - (npConf.getEmbeddingParameterM() - 1) * npConf.getDelayParameterV() < 2) //maxK < 1 means not predictable
        {
            //System.err.println("The original time series (length = " + originalTimeSeriesLength + ") is not predictable, returning 1.0 for PE");
            return -1.0;
        } // if



        //we make train test split of 95% and 5% or 90% and 10%, then check whether first half is predictable
        //calculate the length of first and second part
        int lengthOfSecondPart = (int) (originalTimeSeriesLength * testSplitFraction);
        if(lengthOfSecondPart < 1) lengthOfSecondPart = 1; // if smaller than 1, make it one
        int lengthOfFirstPart = originalTimeSeriesLength - lengthOfSecondPart;

        //if by integer division accidentally first part and second part have same size, make second part one smaller
        //and first part one bigger
        if(lengthOfFirstPart == lengthOfSecondPart)
        {
            lengthOfSecondPart -= 1;
            lengthOfFirstPart += 1;
        } // if



        //if first part is not predictable, then return 1.0 as PE;
        //number of embedding vectors that will be generated based on first part
        //cannot be smaller than 2, since we should have at least one neighbor of betta_N
        //in embedding space for predicting future visits;
        //if it is smaller than 2, return 1 meaning that time series is not predictable
        //in this configuration of embeddingParameter and delayParameterV
        if(lengthOfFirstPart
                - (npConf.getEmbeddingParameterM() - 1)
                * npConf.getDelayParameterV() < 2) // => maxKForFirstPart < 1; where maxKForFirstPart is max #neighbors of B_N
        {
            //System.err.println("First half of the time series is not predictable, returning 1.0 for PE");
            return -1.0;
        } // if


        //visit histories for first part and the second part
        Tuple2<VisitHistory, VisitHistory> firstSecondTuple2 = visitHistory.divide(npConf, trainSplitFraction, testSplitFraction);
        VisitHistory firstPartVisitHistory = firstSecondTuple2._1;
        VisitHistory secondPartVisitHistory = firstSecondTuple2._2;



        //WE CALCULATE EMBEDDING SPACE AND DISTANCE INDEX PAIRS BEFOREHAND
        //then alphaGlobal and indices in distanceIndexPairs will be refined in generateNeighborhood method
        //create an embedding space for the time series of visit history
        Tuple2<Integer, LinkedHashMap<Integer, EmbeddingVector>> indexOfBeta_N_alphaGlobalTuple2
                = createEmbeddingSpace(firstPartVisitHistory.getTimeSeries());
        LinkedHashMap<Integer, EmbeddingVector> alphaGlobal = indexOfBeta_N_alphaGlobalTuple2._2;
        int indexOfBeta_N = indexOfBeta_N_alphaGlobalTuple2._1;
        //int NIndex = alphaGlobal.lastKey();
        EmbeddingVector beta_N = alphaGlobal.get(indexOfBeta_N);


        double stdOfTimeSeriesOfGFirstPartVH = Math.sqrt(Utils.populationVarianceOfTimeSeries(firstPartVisitHistory.getTimeSeries()));
        //calculate the neighborhood beforehand for step count k = 1, then it will refined inside predictAVisitKStepsAhead method
        LinkedHashMap<Integer, EmbeddingVector> neighborhood = epsN(stdOfTimeSeriesOfGFirstPartVH, alphaGlobal, indexOfBeta_N, beta_N);



        //Complexity of TreeMultiMap is the same as TreeMap which O(log(n)), when n elements inserted it is O(n * log(n))
        //Since compareTo() is used for equivalence and ordering then Double can be used as a key,
        //where Double.compareTo() method uses long bits of a Double number;
        //we will sort values of this map which are indices in descending order to collect recent indexed neighbors first
        //Ordering will be as follows: {15.12=[7, 5, 3], 16.1=[2], 17.85=[6, 1]}

        //SetMultimap<Double, Integer> distanceIndexPairs = TreeMultimap.create(Ordering.natural(), Ordering.natural().reverse());
        //ListMultimap<Double, Integer> distIndexPairs = MultimapBuilder.treeKeys().arrayListValues().build();

        //iterate over all B_n in alphaGlobal,
        //alphaGlobal contains keys sorted in ascending order
        //alphaGlobal.remove(indexOfBeta_N); // we do not need beta_N to beta_N distance
        //for(Integer nIndex : alphaGlobal.keySet())
        //{
        //    double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, this.npConf);
        //    distanceIndexPairs.put(distance, nIndex);
        //} // for
        //alphaGlobal.put(indexOfBeta_N, beta_N); // restore beta_N to its original place

        //NO NOTICEABLE PERFORMANCE IMPROVEMENT
        //linked hash multi map is more efficient in terms of get, remove operations
        //it will preserve ordering in passed tree multi map
        //distanceIndexPairs = LinkedHashMultimap.create(distanceIndexPairs);



//        //nIndex and distance pairs; distance is between betta_nIndex and betta_NIndex
//        //we will iterate through alphaGlobal.keySet() in natural order of keys,
//        //therefore for indexDistancePairs insertion order is enough,
//        //where get, put, remove and containsKey takes O(1) time
//        LinkedHashMap<Integer, Double> indexDistancePairs = new LinkedHashMap<Integer, Double>();
//
//        //iterate over all B_n in alphaGlobal,
//        //alphaGlobal contains keys sorted in ascending order
//        //therefore, indexDistancePairs keys will be in ascending order
//        for(Integer nIndex : alphaGlobal.keySet())
//        {
//            //index of B_N is ignored right after for loop
//            double distance = alphaGlobal.get(nIndex).distanceTo(beta_N, this.npConf);
//            indexDistancePairs.put(nIndex, distance);
//        } // for
//        //remove index of B_N from the map, since we find neighbors of B_N
//        indexDistancePairs.remove(indexOfBeta_N); //NIndex);
//
//        //sort map by ascending distance order
//        LinkedHashMap<Integer, Double> indexDistancePairsSortedByAscendingDistance = Utils.sortMapByValues(indexDistancePairs);




        //it is possible that secondPartVisitHistory.getFrequency() can be bigger than
        //firstPartVisitHistory.maxPossibleK(npConf), in this case we choose smaller one
        int numFutureVisitsToPredict
                = firstPartVisitHistory.maxPossibleK(npConf) > secondPartVisitHistory.getFrequency()
                    ? secondPartVisitHistory.getFrequency() : firstPartVisitHistory.maxPossibleK(npConf);

        //predict first part up to the length (frequency) of the second part
        ArrayList<Visit> predictedVisits = this.predictNextVisits(numFutureVisitsToPredict,
                                                                    firstPartVisitHistory,
                                                                    neighborhood,
                                                                    indexOfBeta_N);
        //clear memory
        //distanceIndexPairs.clear();
        alphaGlobal.clear();
        //firstPartVisitHistory.clear();


        //if predicted visit contains null, then epsilon-neighborhood of this visit history is empty
        //so, return -1 as PE, which will be handled in the caller method
        if(predictedVisits.size() == 1 && predictedVisits.get(predictedVisits.size() - 1) == null) // only relevant for 1-step ahead predictions
            return -1.0;


        //first compare predictedVisits with the secondHalf in terms of indices
        /*System.out.println("--------------------------------------------------");
        System.out.println("===== Original visits of the first half =====");
        for (TimeSeriesInstant t: firstHalf)
            System.out.println("{" + location + "; " + t.toStringWithDate() + "}");

        System.out.println("===== Original visits of the second half =====");
        for (TimeSeriesInstant t: secondHalf)
            System.out.println("{" + location + "; " + t.toStringWithDate() + "}");

        System.out.println("===== Predicted visits for the second half =====");
        for(Visit v : predictedVisits)
            System.out.println(v.toStringWithDate());
        System.out.println("--------------------------------------------------\n");*/


        //predictedVisits.forEach(System.out::println);


        //generate time series for predicted visits
        ArrayList<TimeSeriesInstant> predictedSecondPart = new ArrayList<TimeSeriesInstant>();
        for(Visit v : predictedVisits)
        {
            //null visits will always come after the real predicted visits
            //therefore we do not add them to predicted second part
            //in this case only non-null predicted time series instants will be compared against real second part
            //counterparts
            if(v != null)
            //we assume visits are in chronological order
            //time series instant will also be chronological order
            predictedSecondPart.add(v.getTimeSeriesInstant());
        } // for


        //if predictedSecondPart is still empty, then all predicted visits are null, returning -1.0
        if(predictedSecondPart.isEmpty())
            return -1.0;



        ///*
        //calculate MQPE and variance
        double MQPE = MQPE(secondPartVisitHistory.getTimeSeries(), predictedSecondPart);
//        int secondPartVisitHistoryTimeSeriesSize = secondPartVisitHistory.getTimeSeries().size();
//        int numberOfComparisons = secondPartVisitHistoryTimeSeriesSize > predictedSecondPart.size()
//                ? predictedSecondPart.size() : secondPartVisitHistoryTimeSeriesSize;
//        ArrayList<TimeSeriesInstant> varianceToBeFound;
//        if(numberOfComparisons != secondPartVisitHistoryTimeSeriesSize)
//            varianceToBeFound = Utils.head(secondPartVisitHistory.getTimeSeries(), numberOfComparisons);
//        else
//            varianceToBeFound = secondPartVisitHistory.getTimeSeries();


        double variance = Utils.populationVarianceOfTimeSeries(originalTimeSeries);
                        //varianceWithRespectToMeanOfFirstPart(originalTimeSeries,
                        //        secondPartVisitHistory.getTimeSeries(), predictedSecondPart);
                        //= Utils.populationVarianceOfTimeSeries(varianceToBeFound);
                        //= Utils.populationVarianceOfTimeSeries(secondPartVisitHistory.getTimeSeries());


        //given a time series (s_0, s_1, ..., s_N) an predicted values (p_0, p_1, ..., p_N)
        //then mqpe i = 0 to N sum(s_i - p_i)^2 and variance sigma((s_0, s_1, ..., s_N))^2
        //PE = mqpe / variance  4bb360628477284488272b9f9c786957_preprocessed.csv


        //System.out.println("MQPE: " + MQPE + "\nVariance: " + variance);
        //if(Double.isNaN(MQPE / variance)) System.exit(0);

        //return predictability error which is MQPE / variance
        return MQPE / variance;
        //*/


        /*
        double MAPE = MAPE(secondPartVisitHistory.getTimeSeries(), predictedSecondPart);
        double mad = Utils.medianAbsoluteDeviation(originalTimeSeries);
        return MAPE / mad;
        */
    }  //peOfVisitHistory

} // class NextPlace
