/*
 *
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

package org.apache.crail.utils;

public class RingBuffer<T> {
    public T[] elements = null;

    private int size  = 0;
    private int writePos  = 0;
    private int available = 0;

    public RingBuffer(int capacity) {
        this.size = capacity;
        this.elements = (T[]) new Object[capacity];
    }

    public void clear() {
        this.writePos = 0;
        this.available = 0;
    }

    public int capacity() { 
    	return this.size; 
    }
    
    public int size() { 
    	return this.available; 
    }    
    
    public boolean isEmpty(){ 
    	return this.available == 0; 
    }

    public int remaining() {
        return this.size - this.available;
    }

    public boolean add(T element){
        if(available < size){
            if(writePos >= size){
                writePos = 0;
            }
            elements[writePos] = element;
            writePos++;
            available++;
            return true;
        }

        return false;
    }

    public T poll() {
        if(available == 0){
            return null;
        }
        int nextSlot = writePos - available;
        if(nextSlot < 0){
            nextSlot += size;
        }
        T nextObj = elements[nextSlot];
        available--;
        return nextObj;
    }
    
    public T peek() {
        if(available == 0){
            return null;
        }
        int nextSlot = writePos - available;
        if(nextSlot < 0){
            nextSlot += size;
        }
        T nextObj = elements[nextSlot];
        return nextObj;
    } 
}