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

package org.apache.spark.network.server;

import com.google.common.base.Objects;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liweisheng
 * */
public class ReadViewManager {
    private Map<ReadViewKey, PipelineReadView> uniqueId2ReadView = new ConcurrentHashMap<>();

    /**
     * Regist readView
     * @param uniqueId unique read view id
     * @param pipelineManagerId id of pipelineManager which manage read view
     * @param readView
     * */
    public void registPipelineReadView(Long uniqueId, String pipelineManagerId, PipelineReadView readView){
        uniqueId2ReadView.put(new ReadViewKey(uniqueId, pipelineManagerId), readView);
    }

    public PipelineReadView getView(Long uniqueId, String pipelineManagerId){
        return uniqueId2ReadView.get(new ReadViewKey(uniqueId, pipelineManagerId));
    }

    class ReadViewKey{
        public final Long uniqueId;
        public final String pipelineManagerId;

        public ReadViewKey(Long uniqueId, String pipelineManagerId) {
            this.uniqueId = uniqueId;
            this.pipelineManagerId = pipelineManagerId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(uniqueId, pipelineManagerId);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null || !(obj instanceof  ReadViewKey)){
                return false;
            }

            ReadViewKey other = (ReadViewKey)obj;
            return Objects.equal(uniqueId, other.uniqueId)
                    && Objects.equal(pipelineManagerId, other.pipelineManagerId);
        }
    }
}
