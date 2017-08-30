package com.solacesystems.poc;

import java.lang.reflect.Array;
import java.nio.BufferUnderflowException;

/**
 * HACKED RingBuffer to track state objects for sent message.
 *
 * THIS IS NOT THREAD SAFE. I just didn't bother putting in any locking
 * because of the natural sequence of sending messages and receiving acks.
 *
 * @param <T>
 */
class RingBuffer<T> {

    /**
     * Creates a new RingBuffer with a static number of buffer slots available for use.
     * @param clazz Data type of instances to be stored in the buffer.
     * @param capacity Size of the underlying buffer as a number of slots available for storing items.
     */
    public RingBuffer(Class<T> clazz, int capacity) {
        this.capacity = capacity;
        buffer = (T[]) Array.newInstance(clazz, capacity);
    }

    /**
     * Adds a new item to the end of the buffer.
     * @param item additional item
     * @return true if the item could be successfully appended; false if not.
     */
    public boolean append(T item) {
        if (used == capacity) return false;
        buffer[addpos++] = item;
        if (addpos == capacity)
            addpos = 0;
        used++;
        return true;
    }

    /**
     * Removes oldest item from the buffer and returns it.
     * @return oldest instance of T from the buffer.
     * @throws BufferUnderflowException when there are no items available to remove
     */
    public T remove() throws BufferUnderflowException {
        if (used <= 0) throw new BufferUnderflowException();
        T item = buffer[rempos++];
        if (rempos == capacity)
            rempos = 0;
        used--;
        return item;
    }

    /**
     * The total number of items that can be stored at one time. This is static, defined at instantiation.
     * @return the total capacity of the buffer.
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * The number of slots in the buffer that are currently populated with items.
     * @return Instantaneous number of used slots.
     */
    public int used() {
        return used;
    }

    /**
     * The number of slots that are not used and are thus available to be used.
     * @return Instantaneous number of usable slots.
     */
    public int available() {
        return capacity - used();
    }

    final private T[]  buffer;
    final private int  capacity;
    private int used   = 0;
    private int addpos = 0;
    private int rempos = 0;
}
