package com.solacesystems.poc;

import org.junit.Test;

import java.nio.BufferUnderflowException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RingBufferTest {

    @Test
    public void emptyTest() {
        RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 5);
        assertEquals("Capacity is wrong", 5, buffer.capacity());
        assertEquals("Used is wrong", 0, buffer.used());
        assertEquals("Available is wrong", 5, buffer.available());
    }

    @Test
    public void fullTest() {
        RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 5);
        for(int i = 0; i < buffer.capacity(); i++)
            buffer.append(i);
        assertEquals("Capacity is wrong", 5, buffer.capacity());
        assertEquals("Used is wrong", 5, buffer.used());
        assertEquals("Available is wrong", 0, buffer.available());
    }

    @Test
    public void overflowTest() {
        RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 5);
        for(int i = 0; i < buffer.capacity(); i++)
            buffer.append(i);
        assertFalse("Append should fail when full", buffer.append(10));
    }

    @Test(expected = BufferUnderflowException.class)
    public void underflowTest() {
        RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 5);
        Integer i = buffer.remove();
    }

    @Test
    public void ringOverlapTest() {
        RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 10);
        for(int i = 0; i < 7; i++)
            buffer.append(i);
        for(int i = 0; i < 5; i++)
            buffer.remove();
        for(int i = 0; i < 7; i++)
            buffer.append(i+7);
        assertEquals("Capacity is wrong", 10, buffer.capacity());
        assertEquals("Used is wrong", 9, buffer.used());
        assertEquals("Available is wrong", 1, buffer.available());
        assertTrue("One slot left; append should have succeeded", buffer.append(100));
        assertFalse("Buffer is full; append should have failed", buffer.append(101));
    }
}
