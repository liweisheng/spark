package org.apache.spark.network.client;

/**
 * Exception caused by fetch failure of pipeline segment
 *
 * @author liweisheng
 * */
public class PipelineSegmentFetchFailureException extends RuntimeException{
    public PipelineSegmentFetchFailureException(String errMsg){
        super(errMsg);
    }
}
