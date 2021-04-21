package com.sequence.detection.rest.setcontainment;

import com.sequence.detection.rest.model.Sequence;

import java.util.Date;
import java.util.List;

public class VerifyPattern {

    public static boolean verifyPattern(Sequence query, List<String> eventSequence, String strategy) {
        int positionInQuery = 0;
        boolean found = false;
        if (strategy.equals("strict")) {
            for (int i = 0; i < eventSequence.size(); i++) {
                if (positionInQuery < query.getSize() && eventSequence.get(i).equals(query.getEvent(positionInQuery).getName())) {
                    if (positionInQuery == query.getSize() - 1) {
                        found = true;
                    }
                    positionInQuery++;
                } else {
                    positionInQuery = 0; //restart looking for sequence
                }
                if (found) {
                    break;
                }
            }
        } else {
            for (int i = 0; i < eventSequence.size(); i++) {
                if (positionInQuery < query.getSize() && eventSequence.get(i).equals(query.getEvent(positionInQuery).getName())) {
                    if (positionInQuery == query.getSize() - 1) {
                        found = true;
                    }
                    positionInQuery++;
                } //doesnt need a restart
                if (found) {
                    break;
                }
            }
        }
        return found;
    }
}

