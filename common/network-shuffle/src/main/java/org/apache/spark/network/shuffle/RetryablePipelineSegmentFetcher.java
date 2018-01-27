/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.PipelineSegmentReceiveCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenPipeline;
import org.apache.spark.network.shuffle.protocol.PipelineReadViewCreate;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RetryablePipelineSegmentFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(RetryablePipelineSegmentFetcher.class);

    private static ExecutorService pipelineFetcherService = Executors.newCachedThreadPool(
            NettyUtils.createThreadFactory("PipelineSegmentFetcher"));

    private TransportClientFactory clientFactory;
    private final String host;
    private final Integer port;
    private final String pipelineManagerId;
    private final PipelineSegmentFetchingListener listener;
    private final Long startFetchId;

    private final Integer reduceId;
    private final Integer subPipelineIndex;

    private volatile Long lastAckFetchId;

    private volatile Boolean stop = false;

    private int retryCount = 0;
    private final int maxRetries;
    private final int retryWaitTime;

    private PriorityQueue<Long> flightingFetchIds;

    private PipelineReadViewCreate viewCreate = null;

    private volatile PipelineFetcher fetcher;

    public RetryablePipelineSegmentFetcher(
            TransportClientFactory clientFactory,
            TransportConf transportConf,
            String host,
            Integer port,
            String pipelineManagerId,
            Integer subPipelineIndex,
            Integer reduceId,
            PipelineSegmentFetchingListener listener,
            Long startFetchId) {
        this.clientFactory = clientFactory;
        this.host = host;
        this.port = port;
        this.pipelineManagerId = pipelineManagerId;
        this.startFetchId = startFetchId;
        this.subPipelineIndex = subPipelineIndex;
        this.reduceId = reduceId;
        this.listener = listener;
        this.lastAckFetchId = startFetchId;
        this.flightingFetchIds = new PriorityQueue<>();
        this.maxRetries = transportConf.maxIORetries();
        this.retryWaitTime = transportConf.ioRetryWaitTimeMs();
    }

    private class PipelineSegmentCallback implements PipelineSegmentReceiveCallback{
        @Override
        public void onSuccess(Long fetchId, ManagedBuffer buffer) {
            LOG.debug("Successfully fetch data, fetchId:{}, pipelineManagerId:{}, subPipelineIndex:{}, reduceId:{}",
                    fetchId, pipelineManagerId, subPipelineIndex, reduceId);
            RetryablePipelineSegmentFetcher.this.processSuccess(fetchId, buffer);
        }

        @Override
        public void onFailure(Long fetchId, Throwable throwable) {
            LOG.debug("Failed to fetch, fetchId:{}, pipelineManagerId:{}, subPipelineIndex:{}, reduceId:{}",
                    fetchId, pipelineManagerId, subPipelineIndex, reduceId);
            RetryablePipelineSegmentFetcher.this.processFailure(fetchId, throwable);
        }

        @Override
        public void onPipelineEnd(Long fetchId) {
            LOG.debug("Receive pipelineEnd, fetchId:{}, pipelineManagerId:{}, subPipelineIndex:{}, reduceID:{}",
                    fetchId, pipelineManagerId, subPipelineIndex, reduceId);
            RetryablePipelineSegmentFetcher.this.processPipelineEnd(fetchId);
        }
    }

    synchronized private void processSuccess(Long fetchId, ManagedBuffer buffer){
        if(fetchId <= lastAckFetchId){
            LOG.info("Already receive data with fetchId:{}, discard it", fetchId);
            buffer = null;
        }else if(fetchId == lastAckFetchId + 1){
            LOG.debug("Receive fetchId:{}, pipelineManagerId:{}", fetchId, pipelineManagerId);
            listener.onPipelineSegmentFetchSuccess(pipelineManagerId, fetchId, buffer);
            lastAckFetchId = fetchId;
            retryCount = 0;
            Long nextFetchId = lastAckFetchId + 1;
            flightingFetchIds.add(nextFetchId);
            fetcher.fetchPipelineSegment();
        }else{
            LOG.info("Receiver disordered segment, lastAckFetchId:{}, fetchId:{}", lastAckFetchId, fetchId);
            //I think disordered received of fetchId will never happen
        }
    }

    synchronized private void processFailure(Long fetchId, Throwable throwable){
        if(shouldRetry(throwable)){
            initiateRetry(fetchId);
        }else{
            listener.onPipelineSegmentFetchFailure(pipelineManagerId, fetchId, throwable);
        }
    }

    synchronized private void processPipelineEnd(Long fetchId){
        listener.onPipelineEnd(pipelineManagerId, fetchId);
    }

    synchronized private boolean shouldRetry(Throwable e) {
        boolean isIOException = e instanceof IOException
                || (e.getCause() != null && e.getCause() instanceof IOException);
        boolean hasRemainingRetries = retryCount < maxRetries;
        return isIOException && hasRemainingRetries;
    }

    synchronized private void initiateRetry(Long fetchId){
        retryCount += 1;
        LOG.info("Retry to send fetch, fetchId:{}, pipelineManagerId:{}", fetchId, pipelineManagerId);
        if(fetchId != Long.MIN_VALUE){
            flightingFetchIds.add(fetchId);
        }
        pipelineFetcherService.submit(() -> {
            Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
            startFetch();
        });
    }

    public void start(){
        startFetch();
    }

    public void stop(){
        stop = true;
    }

    private void startFetch(){
        fetcher = new PipelineFetcher(new PipelineSegmentCallback());
        try {
            fetcher.createAndStart();
        } catch (Exception e){
            LOG.error(String.format("Exception while beginning open pipeline:{}, subPipelineIndex:{}, reduceId:{}, " +
                            "startFetchId:{}, try number:{}",
                    pipelineManagerId, subPipelineIndex, reduceId, startFetchId, retryCount));

            if(shouldRetry(e)){
                initiateRetry(Long.MIN_VALUE);
            }
        }
    }

    private class PipelineFetcher{
        private TransportClient client;
        private PipelineSegmentCallback callback;

        public PipelineFetcher(PipelineSegmentCallback callback) {
            this.callback = callback;
        }

        public void createAndStart() throws IOException, InterruptedException{
            client = clientFactory.createClient(host, port);
            start();
        }

        private void start(){
            OpenPipeline openPipeline = new OpenPipeline(pipelineManagerId, subPipelineIndex,
                    reduceId, lastAckFetchId);
            client.sendRpc(openPipeline.toByteBuffer(), new RpcResponseCallback(){
                @Override
                public void onSuccess(ByteBuffer response) {
                    viewCreate = (PipelineReadViewCreate) BlockTransferMessage.Decoder.fromByteBuffer(response);
                    LOG.debug("Successfully opened pipeline:{}, subPipelineIndex:{}, reduceId:{}, startFetchId:{}",
                            pipelineManagerId, subPipelineIndex, reduceId, startFetchId);

                    lastAckFetchId = viewCreate.startSyncId;
                    Long nextFetchId = lastAckFetchId + 1;
                    flightingFetchIds.add(nextFetchId);
                    //TODO:successfully open pipeline, start continually fetch data before stop()
                    fetchPipelineSegment();
                }

                @Override
                public void onFailure(Throwable e) {
                    callback.onFailure(Long.MIN_VALUE, e);
                }
            });
        }

        public void fetchPipelineSegment(){
            Long nextId = flightingFetchIds.remove();
            LOG.debug("Send fetch, fetchId:{}, pipelineManagerId:{}", nextId, pipelineManagerId);
            client.fetchPipelineSegment(pipelineManagerId, viewCreate.readViewId, nextId, callback);
        }
    }
}
