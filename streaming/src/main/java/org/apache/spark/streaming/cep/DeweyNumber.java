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

import java.io.Serializable;
import java.util.Arrays;

public class DeweyNumber implements Serializable{
    private static final long serialVersionUID = -3566099596695675894L;

    private int[] internalNums;

    public DeweyNumber(int[] nums){
        this.internalNums = nums;
    }

    public DeweyNumber(DeweyNumber deweyNumber){
        this.internalNums= Arrays.copyOf(deweyNumber.internalNums, deweyNumber.internalNums.length);
    }

    public DeweyNumber addStage(){
        int[] newNums = Arrays.copyOf(this.internalNums, this.internalNums.length + 1);
        newNums[newNums.length - 1] = 0;
        return new DeweyNumber(newNums);
    }

    public DeweyNumber increase(){
        int[] newNums = Arrays.copyOf(this.internalNums, this.internalNums.length);
        newNums[newNums.length - 1] += 1;
        return new DeweyNumber(newNums);
    }

    public int length(){return internalNums.length;}

    public boolean isCompatibaleWith(DeweyNumber other){
        if(other.length() > this.length()) {
            return false;
        }else if(other.length() == this.length()){
            int lastIndex = length() - 1;
            for(int i = 0; i < lastIndex; ++i){
                if(other.internalNums[i] != this.internalNums[i]){
                    return false;
                }
            }

            return this.internalNums[lastIndex] >= other.internalNums[lastIndex];
        }else{
            int lastIndex = other.length();
            for(int i = 0; i < lastIndex; ++i){
                if(other.internalNums[i] != this.internalNums[i]){
                    return false;
                }
            }
        }

        return true;
    }

    public static DeweyNumber createFromInts(int ...ints){
        return new DeweyNumber(ints);
    }

    /**
     * Create from string like '1.0.0'
     * */
    public static DeweyNumber createFromString(String numStr){
        String[] numSplits = numStr.split("\\.");

        if(numSplits.length == 0){
            return createFromInts(Integer.parseInt(numStr));
        }else{
            int[] nums = new int[numSplits.length];
            for(int i = 0; i < numSplits.length; ++i){
                nums[i] = Integer.parseInt(numSplits[i]);
            }

            return createFromInts(nums);
        }
    }
}
