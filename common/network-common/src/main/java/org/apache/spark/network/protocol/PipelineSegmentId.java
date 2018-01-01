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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;

/**
 * Unique identifier for a particular fetching of pipeline segment.
 * Only used on client side, so there is no need to implement {@link Encodable}
 *
 * @author liweisheng
 * */
public class PipelineSegmentId {
    public final String pipelineManagerId;
    public final Long readViewId;
    public final Long fetchId;

    public PipelineSegmentId(String pipelineManagerId, Long readViewId, Long fetchId) {
        this.pipelineManagerId = pipelineManagerId;
        this.readViewId = readViewId;
        this.fetchId = fetchId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pipelineManagerId, readViewId, fetchId);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof PipelineSegmentId)){
            return false;
        }

        PipelineSegmentId other = (PipelineSegmentId)obj;

        return Objects.equal(pipelineManagerId, other.pipelineManagerId)
                && Objects.equal(readViewId, other.readViewId)
                && Objects.equal(fetchId, other.fetchId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("pipelineManagerId", pipelineManagerId)
                .add("readViewId", readViewId)
                .add("fetchId", fetchId)
                .toString();
    }
}
