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

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

public class OpenPipeline extends BlockTransferMessage {
    public final String pipelineManagerId;
    public final Integer subPipelineIndex;
    public final Integer reduceId;
    public final Long syncId;

    private int hash = 0;

    public OpenPipeline(String pipelineManagerId, Integer subPipelineIndex,
                        Integer reduceId, Long syncId) {
        this.pipelineManagerId = pipelineManagerId;
        this.subPipelineIndex = subPipelineIndex;
        this.reduceId = reduceId;
        this.syncId = syncId;
    }

    @Override
    protected BlockTransferMessage.Type type() {
        return Type.OPEN_PIPELINE;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("pipelineManagerId", pipelineManagerId)
                .add("subPipelineIndex", subPipelineIndex)
                .add("reduceId", reduceId)
                .toString();
    }

    @Override
    public int hashCode() {
        if(hash != 0){
            return hash;
        }

        int h = 1;
        h = h * 31 + pipelineManagerId.hashCode();
        h = h * 31 + subPipelineIndex.hashCode();
        h = h * 31 + reduceId.hashCode();
        h = h * 31 + syncId.hashCode();
        hash = h;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof OpenPipeline)){
            return false;
        }

        OpenPipeline other = (OpenPipeline)obj;

        return Objects.equal(pipelineManagerId, other.pipelineManagerId)
                && Objects.equal(subPipelineIndex, other.subPipelineIndex)
                && Objects.equal(reduceId, other.reduceId)
                && Objects.equal(syncId, other.syncId);
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(pipelineManagerId)
                + 4 * 2 + 8;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, pipelineManagerId);
        buf.writeInt(subPipelineIndex);
        buf.writeInt(reduceId);
        buf.writeLong(syncId);
    }

    public static OpenPipeline decode(ByteBuf buf){
        String pipelineManagerId = Encoders.Strings.decode(buf);
        Integer subpipelineIndex = buf.readInt();
        Integer reduceId = buf.readInt();
        Long syncId = buf.readLong();
        return new OpenPipeline(pipelineManagerId, subpipelineIndex, reduceId, syncId);
    }
}
