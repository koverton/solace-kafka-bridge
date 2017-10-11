package com.solacesystems.poc;

import org.junit.Test;

import java.nio.BufferUnderflowException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RingBufferTest {

    private class TestObj implements HasMsgID {
        public TestObj(long id) {
            this.id = id;
        }
        @Override
        public void setMsgID(long id) {
            this.id = id;
        }
        @Override
        public long getMsgID() {
            return id;
        }
        private long id;
    }

    @Test
    public void emptyTest() {
        RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 5);
        assertEquals("Capacity is wrong", 5, buffer.capacity());
        assertEquals("Used is wrong", 0, buffer.used());
        assertEquals("Available is wrong", 5, buffer.available());
    }

    @Test
    public void fullTest() {
        RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 5);
        for(int i = 0; i < buffer.capacity(); i++)
            buffer.append(new TestObj(i));
        assertEquals("Capacity is wrong", 5, buffer.capacity());
        assertEquals("Used is wrong", 5, buffer.used());
        assertEquals("Available is wrong", 0, buffer.available());
    }

    @Test
    public void overflowTest() {
        RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 5);
        for(int i = 0; i < buffer.capacity(); i++)
            buffer.append(new TestObj(i));
        assertFalse("Append should fail when full", buffer.append(new TestObj(10)));
    }

    @Test(expected = BufferUnderflowException.class)
    public void underflowTest() {
        RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 5);
        TestObj i = buffer.remove();
    }

    @Test
    public void ringOverlapTest() {
        RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 10);
        for(int i = 0; i < 7; i++)
            buffer.append(new TestObj(i));
        for(int i = 0; i < 5; i++)
            buffer.remove();
        for(int i = 0; i < 7; i++)
            buffer.append(new TestObj(i+7));
        assertEquals("Capacity is wrong", 10, buffer.capacity());
        assertEquals("Used is wrong", 9, buffer.used());
        assertEquals("Available is wrong", 1, buffer.available());
        assertTrue("One slot left; append should have succeeded", buffer.append(new TestObj(100)));
        assertFalse("Buffer is full; append should have failed", buffer.append(new TestObj(101)));
    }
}
