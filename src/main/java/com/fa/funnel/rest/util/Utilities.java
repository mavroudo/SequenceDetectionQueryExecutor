package com.fa.funnel.rest.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class Utilities
{
    public static boolean isValidInteger(String strInt)
    {
        try {
            Integer.parseInt(strInt);
        }
        catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isValidDate(String strDate)
    {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setLenient(false);
            sdf.parse(strDate);

        } 
        catch (ParseException | NullPointerException e) {
            return false;
        }
        return true;
    }

    public static String getToday()
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return formatter.format(LocalDate.now());
    }
    
    public static String toISO_UTC(Date date) 
    {
        DateFormat dateisoutc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateisoutc.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateisoutc.format(date);
    }
}