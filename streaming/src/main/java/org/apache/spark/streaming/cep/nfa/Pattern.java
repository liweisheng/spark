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


import org.apache.spark.streaming.cep.Time;
import org.apache.spark.streaming.cep.conditions.AndCondition;
import org.apache.spark.streaming.cep.conditions.IterableCondition;
import org.apache.spark.streaming.cep.conditions.OrCondition;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @param <T> type of value
 * */
public class Pattern<T> implements Serializable {
    private String name;

    private Time withInTime = Time.MAX_TIME;
    private IterableCondition<T> condition;

    private Pattern<T> previousPattern;

    private ConsumingPolicy consumingPolicy = ConsumingPolicy.STRICT;
    private QuantifierProperty quantifierProperty = QuantifierProperty.ONLY_ONE;

    private int times;

    private Pattern(String name){
        this.name = name;
    }

    private Pattern(String name, Pattern<T> previousPattern, ConsumingPolicy consumingPolicy){
        this.name = name;
        this.previousPattern = previousPattern;
        this.consumingPolicy = consumingPolicy;
    }

    public static <E> Pattern<E> start(String name){
        return new Pattern<E>(name);
    }

    public Pattern<T> zeroOrOne(){
        this.quantifierProperty = QuantifierProperty.ZERO_OR_ONE;
        return this;
    }

    public Pattern<T> zeroOrMore(){
        this.quantifierProperty = QuantifierProperty.ZERO_OR_MORE;
        return this;
    }

    public Pattern<T> oneOrMore() {
        this.quantifierProperty = QuantifierProperty.ONE_OR_MORE;
        return this;
    }

    public Pattern<T> times(int times){
        this.quantifierProperty = QuantifierProperty.TIMES;
        this.times = times;
        return this;
    }

    public Pattern<T> where(IterableCondition<T> condition){
        //should check not null
        if(this.condition == null){
            this.condition = condition;
        }else{
            this.condition = AndCondition.<T>newAndCondition(this.condition)
                    .and(condition);
        }
        return this;
    }

    public Pattern<T> or(IterableCondition<T> condition){
        //should check not null
        if(this.condition == null){
            this.condition = condition;
        }else{
            this.condition = OrCondition.<T>newOrCondition(condition)
                    .or(this.condition);
        }

        return this;
    }

    public Pattern<T> next(final String name){
        return new Pattern<T>(name, this, ConsumingPolicy.STRICT);
    }

    public Pattern<T> notNext(final String name) {
        if(quantifierProperty == QuantifierProperty.ZERO_OR_ONE ||
                quantifierProperty == QuantifierProperty.ZERO_OR_ONE){
            throw new UnsupportedOperationException("Negation pattern after " +
                    "ZERO_OR_ONE or ZERO_OR_MORE quantifier property is not supported yet");
        }
        return new Pattern<T>(name, this, ConsumingPolicy.NOT_NEXT);
    }

    public Pattern<T> followedBy(final String name){
        return new Pattern<T>(name, this, ConsumingPolicy.SKIP_TILL_NEXT);
    }

    public Pattern<T> notFollowedBy(final String name){
        if(quantifierProperty == QuantifierProperty.ZERO_OR_ONE ||
                quantifierProperty == QuantifierProperty.ZERO_OR_ONE){
            throw new UnsupportedOperationException("Negation pattern after " +
                    "ZERO_OR_ONE or ZERO_OR_MORE quantifier property is not supported yet");
        }
        return new Pattern<T>(name, this, ConsumingPolicy.NOT_FOLLOW);
    }

    public Pattern<T> followedByAny(final String name){
        return new Pattern<T>(name, this, ConsumingPolicy.SKIP_TILL_ANY);
    }

    public Pattern<T> within(long duration, TimeUnit timeUnit){
        this.withInTime = new Time(timeUnit, duration);
        return this;
    }

    public String getName() {
        return name;
    }

    public Time getWithInTime() {
        return withInTime;
    }

    public IterableCondition<T> getCondition() {
        return condition;
    }

    public Pattern<T> getPreviousPattern() {
        return previousPattern;
    }

    public boolean hasPrevious(){
        return previousPattern != null;
    }

    public ConsumingPolicy getConsumingPolicy() {
        return consumingPolicy;
    }

    public boolean isNegation(){
        return consumingPolicy == ConsumingPolicy.NOT_FOLLOW ||
                consumingPolicy == ConsumingPolicy.NOT_NEXT;
    }

    public QuantifierProperty getQuantifierProperty() {
        return quantifierProperty;
    }

    public int getTimes() {
        return times;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof Pattern)){
            return false;
        }

        return this.name.equals(((Pattern)obj).name);
    }
}
