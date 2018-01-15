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

package org.apache.spark.streaming.cep.conditions;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class OrCondition<T> implements IterableCondition<T> {
    private List<IterableCondition<T>> subConditions = new LinkedList<>();

    private OrCondition(IterableCondition<T> condition){
        subConditions.add(condition);
    }

    public static <E> OrCondition<E> newOrCondition(IterableCondition condition){
        return new OrCondition<E>(condition);
    }

    public OrCondition<T> or(List<IterableCondition<T>> conditions){
        subConditions.addAll(conditions);
        return this;
    }

    public OrCondition<T> or(IterableCondition<T> condition){
        this.subConditions.add(condition);
        return this;
    }

    @Override
    public boolean match(T value, Iterator<T> values) {
        for(IterableCondition condition : subConditions){
            if(condition.match(value, values)){
                return true;
            }
        }

        return false;
    }
}
