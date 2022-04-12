package com.sequence.detection.rest.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseGroups {

    /**
     * Returning the groups of trace identifications. If a trace id was not find in the string it will not be inserted
     * in the groups.
     * The format of the expected input string is [(x1,x2,x3-x4),(x5-x9),...]
     * @param input
     * @return
     */
    static public List<List<Integer>> parse(String input){
        List<List<Integer>> response = new ArrayList<>();
        if(input.equals("")){
            return response;
        }
        Matcher m = Pattern.compile("\\(.*?\\)").matcher(input);
        try{
            while(m.find()){
                String s = m.group();
                List<Integer> n= new ArrayList<>();
                String[] ids = s.subSequence(1,s.length()-1).toString().split(",");
                for(String id:ids){
                    if(id.contains("-")){
                        String[] numbers = id.split("-");
                        for(int i = Integer.parseInt(numbers[0]); i<=Integer.parseInt(numbers[1]);i++){
                            n.add(i);
                        }
                    }else{
                        n.add(Integer.parseInt(id));
                    }
                }
                response.add(n);
            }
            return response;
        }catch(Exception e){
            return null;
        }
    }
}
