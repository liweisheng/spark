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
import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Request to fetch a segment of a pipeline. This will correspond to a single
 * {@link ResponseMessage} (either success or failure)
 *
 * @author liweisheng
 * */
final public class PipelineSegmentFetchRequest extends AbstractMessage implements RequestMessage{
    public final String pipelineManagerId;
    public final Long readViewId;
    public final Long fetchId;

    public PipelineSegmentFetchRequest(String pipelineManagerId, Long readViewId, Long fetchId) {
        this.pipelineManagerId = pipelineManagerId;
        this.readViewId = readViewId;
        this.fetchId = fetchId;
    }

    @Override
    public Type type() {
        return Type.PipelineSegmentFetchRequest;
    }

    @Override
    public int encodedLength() {
        return 8 * 2 + Encoders.Strings.encodedLength(pipelineManagerId);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, pipelineManagerId);
        buf.writeLong(readViewId);
        buf.writeLong(fetchId);
    }

    public static PipelineSegmentFetchRequest decode(ByteBuf buf){
        String pipelineManagerId = Encoders.Strings.decode(buf);
        Long readViewId = buf.readLong();
        Long fetchId = buf.readLong();

        return new PipelineSegmentFetchRequest(pipelineManagerId, readViewId, fetchId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pipelineManagerId, readViewId, fetchId);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof PipelineSegmentFetchRequest)){
            return false;
        }

        PipelineSegmentFetchRequest other = (PipelineSegmentFetchRequest)obj;

        return  Objects.equal(pipelineManagerId, other.pipelineManagerId)
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
