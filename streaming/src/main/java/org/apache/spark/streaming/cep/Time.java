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

import java.util.concurrent.TimeUnit;

/**
 * @author liweisheng
 */
public class Time implements Comparable<Time>{
    public static final Time MAX_TIME = Time.ofMillis(Long.MAX_VALUE);

    private TimeUnit timeUnit;
    private long value;

    public Time(TimeUnit timeUnit, long value) {
        this.timeUnit = timeUnit;
        this.value = value;
    }

    public long toMillis(){
        return timeUnit.toMillis(value);
    }

    public static Time ofDays(long value){
        return new Time(TimeUnit.DAYS, value);
    }

    public static Time ofHours(long value){
        return new Time(TimeUnit.HOURS, value);
    }

    public static Time ofMinutes(long value){
        return new Time(TimeUnit.MINUTES, value);
    }

    public static Time ofSeconds(long value){
        return new Time(TimeUnit.SECONDS, value);
    }

    public static Time ofMillis(long value){return new Time(TimeUnit.MILLISECONDS, value);}

    @Override
    public int compareTo(Time o) {
        long otherValue = o.toMillis();
        long thisValue = this.toMillis();

        return thisValue < otherValue ? -1 : ((thisValue > otherValue) ? 1 : 0);
    }
}
