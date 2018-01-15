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
import org.apache.spark.streaming.cep.conditions.IterableCondition;
import org.apache.spark.streaming.cep.conditions.NotCondition;
import org.apache.spark.streaming.cep.conditions.TrueCondition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @param <T> type of event NFA matches
 * */
public class Compiler<T> {
    private final String FINAL_STATE = "_$FINAL";
    private final String START_STATE = "_$START";
    private final String NORMAL_STATE = "_$NORMAL";
    private final String STOP_STATE = "_$STOP";
    private final String IGNORE_STATE = "_$IGNORE";

    private State<T> stopState = null;

    private Set<String> usedStateNames = new HashSet<>();
    private Set<String> usedPatternName = new HashSet<>();
    private Map<Pattern, Pattern> notFollow2Positive = new HashMap<>();

    private Pattern<T> pattern;

    private Pattern<T> currentPattern;

    private Set<State<T>> compiledStates = new HashSet<>();

    private Time timeWindow = Time.MAX_TIME;

    private Compiler(Pattern pattern) {
        this.pattern = pattern;
    }

    public static <E> Compiler newCompiler(
        Pattern<E> pattern){
        Compiler compiler = new Compiler(pattern);
        return compiler;
    }

    public NFAFactory<T> compile(){
        currentPattern = pattern;
        if(currentPattern.getWithInTime().compareTo(timeWindow) < 0){
            timeWindow = currentPattern.getWithInTime();
        }
        State<T> finalState = createFinalState();
        State<T> middleState = createMiddleState(finalState);
        State<T> startState =  createStartState(middleState);

        return new NFAFactory<T>(compiledStates, startState, finalState, timeWindow, notFollow2Positive);
    }

    private State<T> createStartState(State<T> targetState){
        String patternName = currentPattern.getName();
        if(usedPatternName.contains(patternName)){
            throw new IllegalArgumentException("Duplicated pattern name '" + patternName + "' is not supported yet, " +
                    "please use another pattern name");
        }
        usedPatternName.add(patternName);
        State<T> startState = createNormalState(targetState);

        startState.setType(State.StateType.START);
        startState.setName(createStateName(currentPattern.getName(), State.StateType.START));
        if(currentPattern.getWithInTime().compareTo(timeWindow) < 0){
            timeWindow = currentPattern.getWithInTime();
        }

        return startState;
    }

    private State<T> createMiddleState(State<T> targetState){
        State<T> retState = targetState;
        while(currentPattern !=null && currentPattern.hasPrevious()){
            if(currentPattern.getWithInTime().compareTo(timeWindow) < 0){
                timeWindow = currentPattern.getWithInTime();
            }

            String patternName = currentPattern.getName();
            if(usedPatternName.contains(patternName)){
                throw new IllegalArgumentException("Duplicated pattern name '" + patternName + "' is not supported yet, " +
                        "please use another pattern name");
            }
            usedPatternName.add(patternName);

            if(currentPattern.getConsumingPolicy() == ConsumingPolicy.NOT_FOLLOW){
                Pattern<T> previousPositivePattern = getPreviousPositivePattern();
                notFollow2Positive.put(currentPattern, previousPositivePattern);
                //DO NOTHING
            } else if(currentPattern.getConsumingPolicy() == ConsumingPolicy.NOT_NEXT){
                State<T> stopState = createStopState();
                State<T> currentState = createState(State.StateType.NORMAL);
                IterableCondition<T> stopCondition = currentPattern.getCondition();

                currentState.proceed(retState, new NotCondition<T>(stopCondition));
                currentState.proceed(stopState, stopCondition);
                retState = currentState;
            } else{
                retState = createNormalState(retState);
            }


            currentPattern = currentPattern.getPreviousPattern();
        }

        return retState;
    }

    private State<T> createNormalState(State<T> targetState){
        QuantifierProperty quant = currentPattern.getQuantifierProperty();
        if(quant == QuantifierProperty.ZERO_OR_ONE){
            State<T> currentState = createState(State.StateType.NORMAL);

            currentState.take(targetState, currentPattern.getCondition());

            IterableCondition<T> proceedCondition = new TrueCondition<T>();
            currentState.proceed(targetState, proceedCondition);

            IterableCondition<T> ignoreCondition = getIgnoreCondition();
            if(ignoreCondition != null){
                State<T> ignoreState = createIgnoreState(ignoreCondition, currentPattern.getCondition(), targetState);
                currentState.ignore(ignoreState, ignoreCondition);
            }
            return currentState;
        }else if(quant == QuantifierProperty.ONLY_ONE){
            State<T> currentState = createState(State.StateType.NORMAL);

            IterableCondition<T> takeCondition = currentPattern.getCondition();

            currentState.take(targetState, takeCondition);

            IterableCondition<T> ignoreCondition = getIgnoreCondition();

            if(ignoreCondition != null){
                State<T> ignoreState = createIgnoreState(ignoreCondition, currentPattern.getCondition(), targetState);
                currentState.ignore(ignoreState, ignoreCondition);
            }
            return currentState;
        }else if(quant == QuantifierProperty.ZERO_OR_MORE){
            State<T> currentState = createState(State.StateType.NORMAL);

            IterableCondition<T> takeCondition = currentPattern.getCondition();

            currentState.take(takeCondition);

            IterableCondition<T> proceedCondition = new TrueCondition<T>();
            currentState.proceed(targetState, proceedCondition);

            IterableCondition<T> ignoreCondition = getIgnoreCondition();
            if(ignoreCondition != null){
                State<T> ignoreState = createIgnoreState(ignoreCondition, currentPattern.getCondition(), currentState);
                currentState.ignore(ignoreState, ignoreCondition);
            }
            return currentState;
        }else if(quant == QuantifierProperty.ONE_OR_MORE){
            State<T> firstState = createState(State.StateType.NORMAL);

            State<T> loopState = createLoopState(targetState);

            IterableCondition<T> takeCondition = currentPattern.getCondition();

            firstState.take(loopState, takeCondition);

            IterableCondition<T> ignoreCondition = getIgnoreCondition();
            if(ignoreCondition != null){
                State<T> ignoreState = createIgnoreState(ignoreCondition, currentPattern.getCondition(), loopState);
                firstState.ignore(ignoreState, ignoreCondition);
            }
            return firstState;
        }else if(quant == QuantifierProperty.TIMES){
            State<T> currentState = createState(State.StateType.NORMAL);

            IterableCondition<T> takeCondition = currentPattern.getCondition();

            currentState.take(targetState, takeCondition);

            IterableCondition<T> ignoreCondition = getIgnoreCondition();
            if(ignoreCondition != null){
                State<T> ignoreState = createIgnoreState(ignoreCondition, takeCondition, targetState);
                currentState.ignore(ignoreState, ignoreCondition);
            }

            for(int i = 0; i < currentPattern.getTimes() - 1; ++i){
                State<T> forwardState = createState(State.StateType.NORMAL);
                forwardState.take(currentState, takeCondition);

                ignoreCondition = getIgnoreCondition();
                if(ignoreCondition != null){
                    State<T> ignoreState = createIgnoreState(ignoreCondition, takeCondition, forwardState);
                    forwardState.ignore(ignoreState, ignoreCondition);
                }

                currentState = forwardState;
            }
            return currentState;
        }

        return null;
    }

    private State<T> createLoopState(State<T> targetState){
        State<T> firstState = createState(State.StateType.NORMAL);

        IterableCondition<T> takeCondition = currentPattern.getCondition();
        firstState.take(takeCondition);

        IterableCondition<T> proceedCondition = new TrueCondition<T>();
        firstState.proceed(targetState, proceedCondition);

        IterableCondition<T> ignoreCondition = getIgnoreCondition();
        if(ignoreCondition != null){
            State<T> ignoreState = createIgnoreState(ignoreCondition, takeCondition, firstState);
            firstState.ignore(ignoreState, ignoreCondition);
        }

        return firstState;
    }

    private State<T> createIgnoreState(
        IterableCondition<T> ignoreCondition,
        IterableCondition<T> takeCondition,
        State<T> takeTarget){
        State<T> ignoreState = createState(State.StateType.IGNORE);
        ignoreState.ignore(ignoreCondition);
        ignoreState.take(takeTarget, takeCondition);
        return ignoreState;
    }

    private State<T> createStopState(){
        if(stopState == null){
            stopState = new State<T>(STOP_STATE, State.StateType.STOP);
        }
        return stopState;
    }

    private IterableCondition<T> getIgnoreCondition(){
        switch (currentPattern.getConsumingPolicy()){
            case STRICT:
                return null;
            case SKIP_TILL_ANY:
                return new TrueCondition<T>();
            case SKIP_TILL_NEXT:
                return new NotCondition<T>(currentPattern.getCondition());
        }

        return null;
    }

    private State<T> createState(State.StateType type){
        String patternName = currentPattern.getName();
        State<T> state = new State<T>(
            createStateName(patternName, type),
            type);
        compiledStates.add(state);
        return state;
    }

    public State<T> createFinalState(){
         State<T> finalState = new State<T>(
             createFinalStateName(),
             State.StateType.FINAL);

         compiledStates.add(finalState);
         return finalState;
    }

    private String createStateName(String patternName, State.StateType stateType){
        switch (stateType){
            case IGNORE:
                return createIgnoreStateName(patternName);
            case START:
                return createStartStateName(patternName);
            case NORMAL:
                return createMiddleStateName(patternName);
            case FINAL:
                return createFinalStateName();
        }

        //will not reach here
        return "";
    }

    private String createFinalStateName(){
        return "#" + FINAL_STATE;
    }

    private String createStartStateName(String patternName){
        return patternName +"#" + START_STATE;
    }

    private String createMiddleStateName(String patternName){
        int i = 1;
        String stateName = patternName + "#" + i + NORMAL_STATE;
        while(usedStateNames.contains(stateName)){
            stateName = patternName + "#" + (++i) + NORMAL_STATE;
        }

        usedStateNames.add(stateName);
        return stateName;
    }

    private String createIgnoreStateName(String patternName){
        int i = 1;
        String stateName = patternName + "#" + i + IGNORE_STATE;
        while(usedStateNames.contains(stateName)){
            stateName = patternName + "#" + (++i) + IGNORE_STATE;
        }
        return stateName;
    }

    private Pattern<T> getPreviousPositivePattern(){
        Pattern<T> previousPattern = currentPattern.getPreviousPattern();
        while(previousPattern != null){
            if(!previousPattern.isNegation()){
                return previousPattern;
            }

            previousPattern = previousPattern.getPreviousPattern();
        }

        return null;
    }

    public static class NFAFactory<T> {
        private Set<State<T>> states;

        private State<T> startState;
        private State<T> finalState;
        private Time timeWindow;
        private Map<Pattern, Pattern> notFollow2Positive;

        public NFAFactory(
            Set<State<T>> states,
            State<T> startState,
            State<T> finalState,
            Time timeWindow,
            Map<Pattern, Pattern> notFollow2Positive) {
            this.states = states;
            this.startState = startState;
            this.finalState = finalState;
            this.timeWindow = timeWindow;
            this.notFollow2Positive = notFollow2Positive;
        }

        public NFA<T> createNFA(){
            return new NFA<T>(states, startState, timeWindow, notFollow2Positive);
        }
    }
}
