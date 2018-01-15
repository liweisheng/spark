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


import org.apache.spark.streaming.cep.DeweyNumber;
import org.apache.spark.streaming.cep.SharedBuffer;
import org.apache.spark.streaming.cep.Time;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NFARunningInstance<T> {
    private State<T> currentState;
    private SharedBuffer<T> sharedBuffer;

    private SharedBuffer.ForwardEdge edge;

    private DeweyNumber deweyNumber;
    private Time timeWindow;

    //start timestamp of this running instance, a new copied instance from this instance do not change startTime
    private long startTime;

    //start timestamp of first event match start state
    private long startEventTime;

    public NFARunningInstance(
            State<T> currentState,
            SharedBuffer<T> sharedBuffer,
            Time timeWindow,
            long startEventTimestamp) {
        this.currentState = currentState;
        this.sharedBuffer = sharedBuffer;
        this.deweyNumber = createStartDeweyNumber();
        this.timeWindow = timeWindow;
        this.startTime = System.currentTimeMillis();
        this.startEventTime = startEventTimestamp;
    }

    //copy constructor
    public NFARunningInstance(NFARunningInstance other){
        this.currentState = other.currentState;
        this.sharedBuffer = other.sharedBuffer;
        this.deweyNumber = other.deweyNumber;
        this.timeWindow = other.timeWindow;
        this.startTime = other.startTime;
        this.startEventTime = other.startEventTime;
        this.edge =
            (other.edge == null ? null :
                    new SharedBuffer.ForwardEdge(other.edge.getDeweyNumber(), other.edge.getPreviousEntry()));
    }

    private NFARunningInstance(){}

    public NFARunningInstance<T> reserveOld(){
        NFARunningInstance<T> old = new NFARunningInstance<T>();
        old.currentState = currentState;
        old.sharedBuffer = sharedBuffer;
        old.edge =
            (edge == null ? null : new SharedBuffer.ForwardEdge(edge.getDeweyNumber(), edge.getPreviousEntry()));
        old.deweyNumber = deweyNumber;
        old.timeWindow = timeWindow;
        old.startTime = startTime;
        old.startEventTime = startEventTime;
        return old;
    }

    public State<T> getCurrentState() {
        return currentState;
    }

    public void setCurrentState(State<T> currentState) {
        this.currentState = currentState;
    }

    public DeweyNumber getDeweyNumber() {
        return deweyNumber;
    }

    public void setDeweyNumber(DeweyNumber deweyNumber) {
        this.deweyNumber = deweyNumber;
    }

    public Time getTimeWindow() {
        return timeWindow;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStartEventTime() {
        return startEventTime;
    }

    public List<T> alreadyMatchedSeq(){
        SharedBuffer.ReferenceCountBufferEntry entry = edge == null ? null : edge.getPreviousEntry();
        return entry == null? new LinkedList<T>(): entry.getOwnerBuffer().extractEventSeq(entry, deweyNumber);
    }

    public Map<String, LinkedList<T>> classifiedMatchedSeq(){
        SharedBuffer.ReferenceCountBufferEntry entry = edge == null ? null : edge.getPreviousEntry();
        return entry == null ? new HashMap<>() : entry.getOwnerBuffer().extractClassifiedEventSeq(entry, deweyNumber);
    }

    public void takeValue(T value, DeweyNumber deweyNumber, boolean ignore){
        this.deweyNumber = deweyNumber;
        if(edge == null && !ignore){
            SharedBuffer.ReferenceCountBufferEntry<T> entry =
                    sharedBuffer.addStartEntry(currentState.getName(), value, deweyNumber);

            edge = new SharedBuffer.ForwardEdge(deweyNumber, entry);
        }else{
            SharedBuffer.ReferenceCountBufferEntry<T> entry = sharedBuffer.addEntry(
                    currentState.getName(),
                    value,
                    edge.getPreviousEntry(),
                    deweyNumber,
                    ignore);

            edge.reset(entry, deweyNumber);
        }
    }

    public boolean isFinalState(){
        return currentState != null && currentState.getType() == State.StateType.FINAL;
    }

    public boolean isStopState() {return currentState != null && currentState.isStopState();}

    public T getEvent(){
        if(edge == null){
            return null;
        }else{
            return (T)edge.getPreviousEntry().getValue();
        }
    }

    public String getPatternNameOfRecentUnignore(){
        if(edge == null){
            return null;
        }else{
            SharedBuffer.ReferenceCountBufferEntry<T> entry =
                    this.sharedBuffer.getRecentUnignore(edge.getPreviousEntry(), edge.getDeweyNumber());

            return entry == null ? null : entry.getPatternName();
        }
    }

    //Clean instance if it will never be used again,
    public void clean(){}

    private DeweyNumber createStartDeweyNumber(){
        return this.sharedBuffer.getStartDeweyNumber();
    }
}
