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

import java.util.*;

public class NFA<T> {
    private Set<State<T>> states;

    private State<T> startState;

    private SharedBuffer<T> sharedBuffer = new SharedBuffer<T>();

    private Set<NFARunningInstance<T>> runningInstances = new HashSet<>();

    private Set<NFARunningInstance<T>> waitToStopInstance = new HashSet<>();

    private Time timeWindow;

    private Map<Pattern, Pattern> notFollow2Positive;

    public NFA(
            Set<State<T>> states, State<T> startState,
            Time timeWindow,
            Map<Pattern, Pattern> notFollow2Positive) {
        this.states = states;
        this.startState = startState;
        this.timeWindow = timeWindow;
        this.notFollow2Positive = notFollow2Positive;
    }

    /**
     * return a pair, the first element in the pair is all the instances reach final state, the second element is all
     * the instances reach time out state.
     *
     * @param value event
     * @param timestamp event time
     * */
    public Pair<Set<CompletedNFAInstance<T>>, Set<CompletedNFAInstance<T>>> processElement(T value, final long timestamp){
        //1. should process timeout
        runningInstances.add(newInstance(startState, timestamp));

        Set<NFARunningInstance<T>> finalInstances = new LinkedHashSet<>();
        Set<NFARunningInstance<T>> timeoutInstances = new LinkedHashSet<>();

        Iterator<NFARunningInstance<T>> iter = runningInstances.iterator();

        Set<NFARunningInstance<T>> nextInstances = new HashSet<>();

        while(iter.hasNext()){
            NFARunningInstance instance = iter.next();
            State<T> currentState = instance.getCurrentState();
            iter.remove();
            if(currentState.getType() == State.StateType.FINAL){
                finalInstances.add(instance);
                continue;
            }else{
                Set<NFARunningInstance<T>> generatedInstances = computeNextState(instance, value);
                if(generatedInstances.isEmpty()){
                    waitToStopInstance.add(instance);
                }

                Iterator<NFARunningInstance<T>> iter1 = generatedInstances.iterator();
                while(iter1.hasNext()){
                    NFARunningInstance instance1 = iter1.next();
                    if(instance1.isFinalState()){
                        finalInstances.add(instance1);
                        iter1.remove();
                    }else if(instance1.isStopState()){
                        waitToStopInstance.add(instance1);
                        iter1.remove();
                    }else if(timestamp - instance1.getStartEventTime() > timeWindow.toMillis()){
                        timeoutInstances.add(instance1);
                        iter1.remove();
                    }
                }
                nextInstances.addAll(generatedInstances);
            }
        }

        runningInstances = nextInstances;
        processNegationMatch();

        doCleanInstance();

        return new Pair<>(convertToCompletedNFAInstance(finalInstances), convertToCompletedNFAInstance(timeoutInstances));
    }


    private void processNegationMatch(){
        Iterator<NFARunningInstance<T>> iter = runningInstances.iterator();
        while(iter.hasNext()){
            NFARunningInstance<T> instance = iter.next();
            for(Map.Entry<Pattern, Pattern> pp : notFollow2Positive.entrySet()){
                Pattern<T> notFollow = pp.getKey();
                Pattern<T> previousPositive = pp.getValue();

                T event = instance.getEvent();
                List<T> alreadyMatchedSeq = instance.alreadyMatchedSeq();
                int size = alreadyMatchedSeq.size();
                if(size == 0){
                    continue;
                }
                if(notFollow.getCondition().match(event, alreadyMatchedSeq.subList(0, size - 1).iterator())){
                    String positivePatternName = instance.getPatternNameOfRecentUnignore();
                    if(previousPositive.getName().equals(positivePatternName)){
                        waitToStopInstance.add(instance);
                        iter.remove();
                    }
                }
            }
        }
    }

    private Set<CompletedNFAInstance<T>> convertToCompletedNFAInstance(Set<NFARunningInstance<T>> instances){
        Set<CompletedNFAInstance<T>> ret = new LinkedHashSet<>();
        for(NFARunningInstance<T> instance : instances){
            CompletedNFAInstance<T> completedNFAInstance = new CompletedNFAInstance<T>(instance.getStartTime(),
                    instance.classifiedMatchedSeq());
            ret.add(completedNFAInstance);
        }
        return ret;
    }

    private Set<NFARunningInstance<T>> computeNextState(NFARunningInstance<T> instance, T value){
        Set<NFARunningInstance<T>> nextInstances = new HashSet<>();
        State<T> currentState = instance.getCurrentState();
        StateTransition<T> takeTransition = currentState.getTakeTransition();
        List<T> alreadyMatchedSeq = instance.alreadyMatchedSeq();
        boolean canTake = false;
        boolean canIgnore = false;
        boolean canProceed = false;

        NFARunningInstance<T> oldInstance = instance.reserveOld();

        if(takeTransition != null){
            if(canTransitTo(takeTransition, value, alreadyMatchedSeq)){
                DeweyNumber deweyNumber = oldInstance.getDeweyNumber();
                State<T> targetState = takeTransition.getTargetState();
                if(targetState == currentState){
                    DeweyNumber newDeweyNumber = deweyNumber.increase();
                    instance.takeValue(value, newDeweyNumber, false);
                }else{
                    DeweyNumber newDeweyNumber = deweyNumber.increase();
                    instance.takeValue(value, newDeweyNumber, false);
                    instance.setCurrentState(targetState);
                    instance.setDeweyNumber(newDeweyNumber.addStage());
                }

                nextInstances.add(instance);

                canTake = true;
            }
        }

        StateTransition<T> ignoreTransition = currentState.getIgnoreTransition();
        if(ignoreTransition != null){
            if(canTransitTo(ignoreTransition, value, alreadyMatchedSeq)){
                DeweyNumber deweyNumber = oldInstance.getDeweyNumber();
                State<T> targetState = ignoreTransition.getTargetState();
                NFARunningInstance<T> newInstance = new NFARunningInstance<T>(oldInstance);

                if(targetState == currentState){
                    DeweyNumber newDeweyNumber = deweyNumber.increase();
                    newInstance.takeValue(value, newDeweyNumber, true);
                    newInstance.setDeweyNumber(newDeweyNumber);
                }else{
                    DeweyNumber newDeweyNumber = deweyNumber.addStage();
                    newInstance.setDeweyNumber(newDeweyNumber);
                    newInstance.setCurrentState(targetState);
                    newInstance.takeValue(value, newDeweyNumber, true);
                }

                nextInstances.add(newInstance);
                canIgnore = true;
            }
        }

        List<StateTransition<T>> proceedTransitions = currentState.getProceedTransition();
        for(StateTransition<T> proceedTransition : proceedTransitions){
            if(proceedTransition != null){
                if(canTransitTo(proceedTransition, value, alreadyMatchedSeq)){
                    DeweyNumber deweyNumber = oldInstance.getDeweyNumber();
                    State<T> targetState = proceedTransition.getTargetState();

                    NFARunningInstance<T> newInstance = new NFARunningInstance<T>(oldInstance);

//                    newInstance.setDeweyNumber(deweyNumber.addStage());
                    newInstance.setCurrentState(targetState);
                    if(newInstance.isFinalState()){
                        nextInstances.add(newInstance);
                    }else {
                        Set<NFARunningInstance<T>> proceedInstances = computeNextState(newInstance, value);
                        nextInstances.addAll(proceedInstances);
                    }

                    canProceed = true;
                }
            }
        }


        return nextInstances;
    }

    //clean instance if the running nfa instance get to stop state
    private void doCleanInstance(){
        for(NFARunningInstance<T> stopInstance: waitToStopInstance){
            //TODO: do clean
        }

        waitToStopInstance.clear();
    }

    private NFARunningInstance<T> newInstance(State<T> state, long startEventTime){
        NFARunningInstance<T> newInstance =  new NFARunningInstance<T>(state, sharedBuffer, timeWindow, startEventTime);
        return newInstance;
    }


    private boolean canTransitTo(StateTransition transition, T value, List<T> alreadyMatchedSeq){
        if(alreadyMatchedSeq == null){
            alreadyMatchedSeq = new LinkedList<T>();
        }
        return transition.getCondition().match(value, alreadyMatchedSeq.iterator());
    }
}
