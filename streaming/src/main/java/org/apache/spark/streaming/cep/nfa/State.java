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

import org.apache.spark.streaming.cep.conditions.IterableCondition;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class State<T> implements Serializable {
    private static final long serialVersionUID = 1797755802784877070L;

    private String name;

    private StateTransition<T> takeTransition;
    private List<StateTransition<T>> proceedTransitions = new LinkedList<>();
    private StateTransition<T> ignoreTransition;
    private StateType type;

    private int times = -1;

    public State(String name, StateType type) {
        //should check not null
        this.name = name;
        this.type = type;
    }

    private void addTransition(
        State<T> targetState,
        IterableCondition<T> condition,
        StateTransition.TransitionAction action){
        StateTransition<T> transition = new StateTransition<T>(
                condition,
                this,
                targetState,
                action);

        if(action == StateTransition.TransitionAction.TAKE){
            takeTransition = transition;
        }else if(action == StateTransition.TransitionAction.IGNORE){
            ignoreTransition = transition;
        }else if(action == StateTransition.TransitionAction.PROCEED){
            proceedTransitions.add(transition);
        }
    }

    public void take(State<T> targetState, IterableCondition<T> condition){
        addTransition(targetState, condition, StateTransition.TransitionAction.TAKE);
    }

    public void take(IterableCondition<T> condition){
        take(this, condition);
    }

    public void proceed(State<T> targetState, IterableCondition<T> condition){
        addTransition(targetState, condition, StateTransition.TransitionAction.PROCEED);
    }

    public void ignore(State<T> targetState, IterableCondition<T> condition){
        addTransition(targetState, condition, StateTransition.TransitionAction.IGNORE);
    }

    public void ignore(IterableCondition<T> ignoreCondition){
        ignore(this, ignoreCondition);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<StateTransition<T>> getProceedTransition(){
        return proceedTransitions;
    }


    public StateTransition<T> getTakeTransition(){
        return takeTransition;
    }

    public StateTransition<T> getIgnoreTransition(){
        return ignoreTransition;
    }

    public StateType getType() {
        return type;
    }

    public void setType(StateType type) {
        this.type = type;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }

    public boolean isStopState(){
        return type == StateType.STOP;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + name.hashCode();
        hash = hash * 31 + type.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof State)){
            return false;
        }

        State other = (State)obj;
        return name.equals(other.name)
                && type.equals(other.type);
    }

    public enum StateType{
        START,
        NORMAL,
        IGNORE,
        FINAL,
        STOP
    }



}
