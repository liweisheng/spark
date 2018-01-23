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

public class StateTransition<T> implements Serializable {
    private IterableCondition<T> condition;
    private State<T> sourceState;
    private State<T> targetState;
    private TransitionAction action;

    public StateTransition(
        IterableCondition<T> condition,
        State<T> sourceState,
        State<T> targetState,
        TransitionAction action) {
        this.condition = condition;
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.action = action;
    }

    public IterableCondition<T> getCondition() {
        return condition;
    }

    public State<T> getSourceState() {
        return sourceState;
    }

    public State<T> getTargetState() {
        return targetState;
    }

    public TransitionAction getAction() {
        return action;
    }

    public enum TransitionAction{
        TAKE,
        IGNORE,
        PROCEED
    }
}
