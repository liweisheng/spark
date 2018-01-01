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
import io.netty.buffer.ByteBuf;

/**
 * Response to {@link PipelineSegmentFetchRequest} when the fetching of a pipeline segment is failed.
 *
 * @author liweisheng
 * */
public class PipelineSegmentFetchFailure extends AbstractMessage implements ResponseMessage{
    public final String pipelineManagerId;
    public final Long readViewId;
    public final Long fetchId;

    public final String errorString;

    public PipelineSegmentFetchFailure(String pipelineManagerId, Long readViewId, Long fetchId, String errorString) {
        this.pipelineManagerId = pipelineManagerId;
        this.readViewId = readViewId;
        this.fetchId = fetchId;
        this.errorString = errorString;
    }

    @Override
    public Type type() {
        return Type.PipelineSegmentFetchFailure;
    }

    @Override
    public int encodedLength() {
        return 8 * 2 + Encoders.Strings.encodedLength(pipelineManagerId) +  Encoders.Strings.encodedLength(errorString);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, pipelineManagerId);
        buf.writeLong(readViewId);
        buf.writeLong(fetchId);
        Encoders.Strings.encode(buf, errorString);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pipelineManagerId, readViewId, fetchId, errorString);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof PipelineSegmentFetchFailure)){
            return false;
        }

        PipelineSegmentFetchFailure other = (PipelineSegmentFetchFailure)obj;

        return Objects.equal(pipelineManagerId, other.pipelineManagerId)
                && Objects.equal(readViewId, other.readViewId)
                && Objects.equal(fetchId, other.fetchId)
                && Objects.equal(errorString, other.errorString);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("pipelineManagerId", pipelineManagerId)
                .add("readViewId", readViewId)
                .add("fetchId", fetchId)
                .add("errorString", errorString)
                .toString();
    }

    public static PipelineSegmentFetchFailure decode(ByteBuf buf) {
        String pipelineManagerId = Encoders.Strings.decode(buf);
        Long readViewId = buf.readLong();
        Long fetchId = buf.readLong();
        String errorString = Encoders.Strings.decode(buf);
        return new PipelineSegmentFetchFailure(pipelineManagerId, readViewId, fetchId, errorString);
    }
}
