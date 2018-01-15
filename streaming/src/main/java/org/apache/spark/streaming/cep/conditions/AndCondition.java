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

public class AndCondition<T> implements IterableCondition<T> {
    private List<IterableCondition<T>> subConditions = new LinkedList<>();

    private AndCondition(IterableCondition<T> condition){
        subConditions.add(condition);
    }

    public static <E> AndCondition<E> newAndCondition(IterableCondition<E> condition) {
        return new AndCondition<E>(condition);
    }

    @Override
    public boolean match(T value, Iterator<T> values) {
        for(IterableCondition subCondition: subConditions){
            if(!subCondition.match(value, values)){
                return false;
            }
        }
        return true;
    }

    public AndCondition<T> and(IterableCondition<T> condition){
        subConditions.add(condition);
        return this;
    }
}
