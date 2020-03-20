package com.fa.funnel.rest.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the output of an {@code status} endpoint query
 * @author Andreas Kosmatopoulos
 */

public class Status {

    private List<String> user;
    private List<String> device;

    public Status() {
        user = new ArrayList<>();
        device = new ArrayList<>();
    }

    public Status(List<String> user, List<String> device) {
        this.user = user;
        this.device = device;
    }

    public void setUser(List<String> user) { this.user = user; }
    public void setDevice(List<String> device) { this.device = device; }

    public List<String> getUser() { return user; }
    public List<String> getDevice() { return device; }

}
