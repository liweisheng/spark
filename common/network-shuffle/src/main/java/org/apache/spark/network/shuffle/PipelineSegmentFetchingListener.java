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

import org.apache.spark.network.buffer.ManagedBuffer;

import java.util.EventListener;

public interface PipelineSegmentFetchingListener extends EventListener {

    /**
     * Called once per successfully fetching of a segment of the pipeline with a unique and monotonously incremental fetchId.
     * After this call returns, data buffer will be released atomatically. If data is processed by another thread, the receiver
     * should retain and release the buffer on their own, or copy to a new buffer.
     * */
    void onPipelineSegmentFetchSuccess(String pipelineManagerId, Long fetchId, ManagedBuffer data);

    /**
     * Called at least one when fetching with fetchId is failed.
     * */
    void onPipelineSegmentFetchFailure(String pipelineManagedId, Long fetchId, Throwable t);
}
