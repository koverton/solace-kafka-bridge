package com.solacesystems.poc;

/**
 * Basic MessageID interface based on Long Integer as the underlying ID storage type.
 */
public interface HasMsgID {

    /**
     * Sets the long integer ID.
     *
     * @param id Long integer ID to set the object value to.
     */
    void setMsgID(long id);

    /**
     * gets the underlying long integer ID from an object.
     *
     * @return Long integer ID value from the object.
     */
    long getMsgID();
}
