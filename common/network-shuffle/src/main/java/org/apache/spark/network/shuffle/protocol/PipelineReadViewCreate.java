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
import io.netty.buffer.ByteBuf;

/**
 * Response to {@link OpenPipeline} when pipeline is successfully opened.
 *
 * @author liweisheng
 * */
public class PipelineReadViewCreate extends BlockTransferMessage{
    public final Long readViewId;
    public final Long startSyncId;

    public PipelineReadViewCreate(Long readViewId, Long startSyncId) {
        this.readViewId = readViewId;
        this.startSyncId = startSyncId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(readViewId, startSyncId);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof PipelineReadViewCreate)){
            return false;
        }

        PipelineReadViewCreate other = (PipelineReadViewCreate)obj;

        return Objects.equal(readViewId, other.readViewId)
                && Objects.equal(startSyncId, other.startSyncId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("pipelineReadViewId", readViewId)
                .add("startSyncId", startSyncId)
                .toString();
    }

    @Override
    public int encodedLength() {
        return 8 * 2;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(readViewId);
        buf.writeLong(startSyncId);
    }

    @Override
    protected Type type() {
        return Type.PIPELINE_READ_VIEW_CREATE;
    }

    public static PipelineReadViewCreate decode(ByteBuf buf){
        Long readViewId = buf.readLong();
        Long startSyncId = buf.readLong();
        return new PipelineReadViewCreate(readViewId, startSyncId);
    }
}
