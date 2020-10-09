package com.sequence.detection.rest.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class UtilitiesTest
{

    @Test
    public void testDateIsNull()
    {
        assertFalse(Utilities.isValidDate(null));
    }

    @Test
    public void testDateIsEmpty()
    {
        assertFalse(Utilities.isValidDate(""));
    }

    @Test
    public void testDates()
    {
        assertTrue(Utilities.isValidDate("2017-10-10"));
        assertTrue(Utilities.isValidDate("2017-12-31"));
        assertTrue(Utilities.isValidDate("2000-03-25"));
        assertTrue(Utilities.isValidDate("2008-02-29"));
        assertFalse(Utilities.isValidDate("2017-02-29"));
        assertFalse(Utilities.isValidDate("02-0212-29"));
        assertFalse(Utilities.isValidDate("0000-00-00"));
        assertFalse(Utilities.isValidDate("10-02-2010"));
    }

    @Test
    public void testIntegerIsNull()
    {
        assertFalse(Utilities.isValidInteger(null));
    }

    @Test
    public void testIntegerIsEmpty()
    {
        assertFalse(Utilities.isValidInteger(""));
    }

    @Test
    public void testIntegers()
    {
        assertTrue(Utilities.isValidInteger("368"));
        assertTrue(Utilities.isValidInteger("10000"));
        assertTrue(Utilities.isValidInteger("0"));
        assertTrue(Utilities.isValidInteger("-32"));
        assertFalse(Utilities.isValidInteger("96-32"));
        assertFalse(Utilities.isValidInteger("96-32"));
        assertFalse(Utilities.isValidInteger("a2"));
        assertFalse(Utilities.isValidInteger("2 "));
    }

}