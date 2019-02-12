package com.rb.nextplace.distributed;

import javax.inject.Singleton;
import java.io.Serializable;

//singleton configuration class to hold m, v, timeT and delaT for NextPlace algorithm
public class NextPlaceConfiguration implements Serializable
{
    //generated serial version uid field
    private static final long serialVersionUID = -3874794134015972146L;

    private static NextPlaceConfiguration npConf;

    //embedding parameter m
    private int embeddingParameterM;

    //delay parameter v
    private int delayParameterV;

    //distance metric to use for finding distance between embedding vectors
    private DistanceMetric distanceMetric;

    //neighborhood in similarity search
    private NeighborhoodType neighborhoodType;

    //constructor
    public NextPlaceConfiguration(int embeddingParameterM, int delayParameterV,
                                   DistanceMetric distanceMetric, NeighborhoodType neighborhoodType)
    {
        this.distanceMetric = distanceMetric;
        this.neighborhoodType = neighborhoodType;

        //do sanity checks
        if(embeddingParameterM < 1)
        {
            System.err.println("Embedding parameter M cannot be smaller than 1; m is adjusted to 1");
            this.embeddingParameterM = 1;
        } // if
        else
            this.embeddingParameterM = embeddingParameterM;



        if(delayParameterV < 1)
        {
            System.err.println("Delay parameter V cannot be smaller than 1; m is adjusted to 1");
            this.delayParameterV = 1;
        } // if
        else
            this.delayParameterV = delayParameterV;
    } // NextPlaceConfiguration


    //getInstance method to get the one and only instance of NextPlace configuration
    public static NextPlaceConfiguration getInstance(int embeddingParameterM, int delayParameterV,
                                                     DistanceMetric distanceMetric, NeighborhoodType neighborhoodType)
    {
        //if the instance is not created yet, create it and return it
        if(npConf == null)
            npConf = new NextPlaceConfiguration(embeddingParameterM, delayParameterV, distanceMetric, neighborhoodType);


        //return one and only configuration instance
        return npConf;
    } // getInstance


    //getter for instance variables
    public int getEmbeddingParameterM() {
        return embeddingParameterM;
    }


    public int getDelayParameterV() {
        return delayParameterV;
    }

    public DistanceMetric getDistanceMetric() { return distanceMetric; }

    public NeighborhoodType getNeighborhoodType() {
        return neighborhoodType;
    }

    public enum NeighborhoodType
    {
        EPSILON_NEIGHBORHOOD
        //other types of neighborhood can be implemented
    } // NeighborType

} // class NextPlaceConfiguration
