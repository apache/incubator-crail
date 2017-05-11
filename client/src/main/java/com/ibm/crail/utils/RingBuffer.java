package com.ibm.crail.utils;

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