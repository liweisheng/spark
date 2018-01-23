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

package org.apache.spark.streaming.cep;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @param <T> type of value this buffer hold
 * @author liweisheng
 */
public class SharedBuffer<T> {
    private final ReferenceCountBufferEntry ORIGIN_ENTRY = new ReferenceCountBufferEntry(null, this, false);

    private final HashMap<String, Bucket> internalBuffer = new HashMap<>();

    private volatile int startDeweyNumber= 0;

    synchronized public ReferenceCountBufferEntry addEntry(
            String key,
            T value,
            ReferenceCountBufferEntry previousEntry,
            DeweyNumber deweyNumber,
            boolean ignore){
        ReferenceCountBufferEntry entry = new ReferenceCountBufferEntry(value, this, ignore);
        return addEntry(key, entry, previousEntry, deweyNumber);
    }

    synchronized public ReferenceCountBufferEntry addEntry(
        String key,
        ReferenceCountBufferEntry entry,
        ReferenceCountBufferEntry previousEntry,
        DeweyNumber deweyNumber){
        ForwardEdge edge = new ForwardEdge(deweyNumber, previousEntry);
        entry.addForwardEdge(edge);

        Bucket bucket = internalBuffer.get(key);
        if(bucket == null){
            bucket = new Bucket(key);
            internalBuffer.put(key, bucket);
        }
        bucket.addEntry(entry);
        return entry;
    }

    synchronized public DeweyNumber getStartDeweyNumber(){
        startDeweyNumber = (startDeweyNumber + 1) % Integer.MAX_VALUE;
        return DeweyNumber.createFromInts(startDeweyNumber, 0);
    }

    synchronized public ReferenceCountBufferEntry addStartEntry(String key, T value, DeweyNumber deweyNumber){
        return addEntry(key, value, ORIGIN_ENTRY, deweyNumber, false);
    }

    public List<T> extractEventSeq(ReferenceCountBufferEntry<T> fromEntry, DeweyNumber deweyNumber){
        LinkedList<T>  eventSeq = new LinkedList<T>();
        ReferenceCountBufferEntry<T> currentEntry = fromEntry;
        DeweyNumber currentDeweyNumber = deweyNumber;

        while(currentEntry != ORIGIN_ENTRY && currentEntry != null){
            eventSeq.addFirst(currentEntry.getValue());
            List<ForwardEdge> forwardEdges = currentEntry.forwardEdges;
            currentEntry = null;
            for(ForwardEdge forwardEdge : forwardEdges){
                if(currentDeweyNumber.isCompatibaleWith(forwardEdge.deweyNumber)){
                    currentEntry = forwardEdge.previousEntry;
                    currentDeweyNumber = forwardEdge.deweyNumber;
                }
            }
        }

        return eventSeq;
    }

    /**
     * Return matched events sequence which classified by their pattern.
     * */
    public Map<String, List<T>> extractClassifiedEventSeq(
            ReferenceCountBufferEntry<T> fromEntry,
            DeweyNumber deweyNumber){
        Map<String, List<T>> ret = new LinkedHashMap<>();
        ReferenceCountBufferEntry<T> currentEntry = fromEntry;
        DeweyNumber currentDeweyNumber = deweyNumber;

        List<T> seqInSameBucket;
        String key;
        while(currentEntry != ORIGIN_ENTRY && currentEntry != null){
            if(!currentEntry.ignore()){
                key = currentEntry.getBucket().name;
                key = key.substring(0, key.lastIndexOf("#"));
                seqInSameBucket = ret.get(key);
                if(seqInSameBucket == null){
                    seqInSameBucket = new LinkedList<T>();
                    ret.put(key, seqInSameBucket);
                }

                ((LinkedList)seqInSameBucket).addFirst(currentEntry.getValue());
            }

            List<ForwardEdge> forwardEdges = currentEntry.forwardEdges;

            for(ForwardEdge forwardEdge : forwardEdges){
                if(currentDeweyNumber.isCompatibaleWith(forwardEdge.deweyNumber)){
                    currentEntry = forwardEdge.previousEntry;
                    currentDeweyNumber = forwardEdge.deweyNumber;
                }
            }
        }

        return ret;
    }

    public ReferenceCountBufferEntry<T> getRecentUnignore(
            ReferenceCountBufferEntry<T> fromEntry,
            DeweyNumber deweyNumber){
        ReferenceCountBufferEntry<T> currentEntry = fromEntry;
        DeweyNumber currentDeweyNumber = deweyNumber;

        while(currentEntry != ORIGIN_ENTRY && currentEntry != null){
            List<ForwardEdge> forwardEdges = currentEntry.forwardEdges;
            for(ForwardEdge forwardEdge : forwardEdges){
                if(currentDeweyNumber.isCompatibaleWith(forwardEdge.deweyNumber)){
                    currentEntry = forwardEdge.previousEntry;
                    currentDeweyNumber = forwardEdge.deweyNumber;
                    if(!currentEntry.ignore){
                        return currentEntry;
                    }
                }
            }
        }

        return null;
    }

    public static class ForwardEdge{
        private DeweyNumber deweyNumber;
        private ReferenceCountBufferEntry previousEntry;

        public ForwardEdge(DeweyNumber deweyNumber, ReferenceCountBufferEntry previousEntry) {
            this.deweyNumber = deweyNumber;
            this.previousEntry = previousEntry;
            previousEntry.incReferenceCount(1);
        }

        public void reset(ReferenceCountBufferEntry entry, DeweyNumber deweyNumber){
            this.deweyNumber = deweyNumber;

            if(previousEntry != null){
                previousEntry.decReferenceCount();
            }

            previousEntry = entry;
            previousEntry.incReferenceCount(1);
        }

        public ReferenceCountBufferEntry getPreviousEntry() {
            return previousEntry;
        }

        public DeweyNumber getDeweyNumber() {
            return deweyNumber;
        }
    }

    class Bucket{
        private final String name;
        private final LinkedHashSet<ReferenceCountBufferEntry<T>> internalBucket;

        public Bucket(String name) {
            this.name = name;
            this.internalBucket = new LinkedHashSet<>();
        }

        public void addEntry(ReferenceCountBufferEntry<T> entry){
            this.internalBucket.add(entry);
            entry.setBucket(this);
        }

        public String getName() {
            return name;
        }
    }

    /**
     * @param <T> type of value
     */
    public static class ReferenceCountBufferEntry<T>{
        private SharedBuffer.Bucket bucket;
        private AtomicInteger referenceCount = new AtomicInteger(0);
        private T value;
        private List<ForwardEdge> forwardEdges;
        private SharedBuffer<T> ownerBuffer;
        private boolean ignore;

        private ReferenceCountBufferEntry(
            T value,
            List<ForwardEdge> forwardEdges,
            SharedBuffer<T> ownerBuffer,
            boolean ignore) {
            this.value = value;
            this.forwardEdges = forwardEdges;
            this.ownerBuffer = ownerBuffer;
            this.bucket = bucket;
            this.ignore = ignore;
        }

        public ReferenceCountBufferEntry(T value, SharedBuffer<T> ownerBuffer, boolean ignore) {
            this(value,  new LinkedList<>(), ownerBuffer, ignore);
        }

        public SharedBuffer<T> getOwnerBuffer() {
            return ownerBuffer;
        }

        public int incReferenceCount(int inc){
            return referenceCount.addAndGet(inc);
        }

        public int decReferenceCount(){
            return this.referenceCount.decrementAndGet();
        }

        public int getReferenceCount() {
            return referenceCount.get();
        }

        public T getValue() {
            return value;
        }

        public void addForwardEdge(ForwardEdge edge){
            this.forwardEdges.add(edge);
        }

        public List<ForwardEdge> getForwardEdges(){
            return this.forwardEdges;
        }

        public SharedBuffer.Bucket getBucket() {
            return bucket;
        }

        public void setBucket(SharedBuffer.Bucket bucket) {
            this.bucket = bucket;
        }

        public boolean ignore(){return this.ignore;}

        public String getPatternName(){
            String bucketName = bucket.getName();
            int hashIndex = bucketName.lastIndexOf('#');
            return bucketName.substring(0, hashIndex);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null || !(obj instanceof ReferenceCountBufferEntry)){
                return false;
            }

            return this == obj;
        }
    }
}
