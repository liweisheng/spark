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

package org.apache.spark.streaming.cep.nfa;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * When a running nfa instance reach final state or just timeout, this running instance will be converted to a
 * CompletedNFAInstance with all events already matched.
 * */
public class CompletedNFAInstance<T> {
    private long startTimeStamp;
    private final Map<String, LinkedList<T>> pattern2Events;

    public CompletedNFAInstance(long startTimeStamp, Map<String, LinkedList<T>> pattern2Events) {
        this.startTimeStamp = startTimeStamp;
        this.pattern2Events = pattern2Events;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public List<T> get(String patternName){
        return pattern2Events.get(patternName);
    }

    public List<String> getPatterns(){
        LinkedList<String> ret = new LinkedList<>();
        for(String patternName: pattern2Events.keySet()){
            ret.addFirst(patternName);
        }

        return ret;
    }

}
