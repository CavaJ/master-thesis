package com.rb.nextplace.distributed;

public enum DistanceMetric
{
    Manhattan,
    Euclidean,
    Chebyshev,
    ComparativeManhattan,
    ComparativeEuclidean,
    ComparativeChebyshev,
    ComparativeLoopingDistance;

    //helper method to generate simple string representation for distance metrics
    public String toSimpleString()
    {
        switch (this)
        {
            case Manhattan: return "d_man";
            case Euclidean: return "d_euc";
            case Chebyshev: return "d_che";
            case ComparativeManhattan: return "d_comman";
            case ComparativeEuclidean: return "d_comeuc";
            case ComparativeChebyshev: return "d_comche";
            case ComparativeLoopingDistance: return "d_comloop";
            default: return "d_unknown";
        } // switch
    } // toSimpleString
} // enum DistanceMetric
