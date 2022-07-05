package com.sequence.detection.rest.groups;

import com.codepoetics.protonpack.StreamUtils;
import com.datastax.driver.core.*;
import com.sequence.detection.rest.model.AugmentedDetail;
import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.Lifetime;
import com.sequence.detection.rest.model.Sequence;
import com.sequence.detection.rest.query.SequenceQueryHandler;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


public class SequenceQueryEvaluatorGroups extends SequenceQueryHandler {
    private List<Set<Integer>> groups;

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryEvaluatorGroups(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name, List<Set<Integer>> groups) {
        super(cluster, session, ks, cassandra_keyspace_name);
        this.groups = groups;
    }

    private class TraceWithTimestamps {
        private Integer event;
        private List<Date> dates;

        public TraceWithTimestamps(String event, List<Date> dates) {
            this.event = Integer.parseInt(event);
            this.dates = dates;
        }

        public Integer getEvent() {
            return event;
        }

        public void setEvent(Integer event) {
            this.event = event;
        }

        public List<Date> getDates() {
            return dates;
        }

        public void setDates(List<Date> dates) {
            this.dates = dates;
        }
    }

    private class EventWithTimestamps {
        private String event;
        private List<Date> dates;

        private Integer groupId;

        public EventWithTimestamps(String event, List<Date> dates, int groupId) {
            this.event = event;
            this.dates = dates;
            Collections.sort(this.dates);
            this.groupId = groupId;
        }

        public String getEvent() {
            return event;
        }

        public void setEvent(String event) {
            this.event = event;
        }

        public List<Date> getDates() {
            return dates;
        }

        public void setDates(List<Date> dates) {
            this.dates = dates;
        }

        public Integer getGroupId() {
            return groupId;
        }

        public void setGroupId(Integer groupId) {
            this.groupId = groupId;
        }
    }

    private class EventWithDate implements Comparable<EventWithDate>{
        private String event;
        private Date time;

        public EventWithDate(String s, Date parse) {
            this.event=s;
            this.time=parse;
        }

        @Override
        public int compareTo(EventWithDate o) {
            return this.time.compareTo(o.time);
        }
    }

    private class CombinedTraces extends Pair<Long,List<EventWithDate>> {
        private Long id;
        private List<EventWithDate> e = new ArrayList<>();

        public CombinedTraces(long index, List<EventWithDate> events) {
            this.id=index;
            this.e=events;
        }

        @Override
        public Long getLeft() {
            return this.id;
        }

        @Override
        public List<EventWithDate> getRight() {
            return this.e;
        }

        @Override
        public List<EventWithDate> setValue(List<EventWithDate> value) {
            Collections.copy(this.e,value);
            return this.e;
        }
    }

    public Map<String, List<Lifetime>> detect(Date start_date, Date end_date, Sequence query, Map<Integer,
            List<AugmentedDetail>> queryDetails, String table_name, boolean returnAll) {
        Map<String, List<Lifetime>> results;
        int l = table_name.split("_").length;
        String table_one = String.join("_", Arrays.copyOfRange(table_name.split("_"), 0, l - 1)) + "_one";
        List<Map<String,EventWithTimestamps>> perGroup = this.queryEvent(query.getList(), queryDetails, table_one);
        results = this.evaluateGroups(perGroup, start_date, end_date, query.getList(),returnAll);
        return results;
    }
    public Map<String, List<Lifetime>> detectNaive(Date start_date, Date end_date, Sequence query, Map<Integer,
            List<AugmentedDetail>> queryDetails, String table_name, boolean returnAll) {
        Map<String, List<Lifetime>> results;
        int l = table_name.split("_").length;
        String table_seq = String.join("_", Arrays.copyOfRange(table_name.split("_"), 0, l - 1)) + "_seq";
        List<CombinedTraces> perGroup =  this.queryEventNaive(groups, table_seq);
        results = this.evaluateGroupsNaive(perGroup,start_date,end_date,query.getList(),returnAll);
        return results;
    }
    private List<Map<String, EventWithTimestamps>> queryEvent(List<Event> events, Map<Integer, List<AugmentedDetail>> queryDetails, String table_one) {
        List<List<EventWithTimestamps>> perEvent = events
                .stream()
                .parallel()
                .map(s -> {
                    ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + table_one +
                            " WHERE event_name = ? ", s.getName());
                    Row row = rs.one();
                    Map<Integer, List<Date>> dates = this.handleRow(row.getList("sequences", String.class), groups);
                    List<EventWithTimestamps> e = StreamUtils.zipWithIndex(groups.stream().parallel())
                            .map(g -> {
                                List<Date> ds = g.getValue().stream().map(x -> dates.getOrDefault(x, new ArrayList<>())).reduce(new ArrayList<>(), (a, b) -> {

                                    a.addAll(b);
                                    return a;
                                });
                                return new EventWithTimestamps(s.getName(), ds, (int) g.getIndex());
                            })
                            .collect(Collectors.toList());
                    return e;
                }).collect(Collectors.toList());

        return StreamUtils.zipWithIndex(groups.stream().parallel())
                .map(g -> {
                    Map<String,EventWithTimestamps> rt= new HashMap<>();
                    perEvent.stream()
                            .flatMap(Collection::stream)
                            .filter(x -> x.getGroupId() == g.getIndex())
                            .forEach(x->rt.put(x.getEvent(),x));
                    return rt;
                }).collect(Collectors.toList());
    }
    private List<CombinedTraces> queryEventNaive(List<Set<Integer>> groups, String table_seq){
        return StreamUtils.zipWithIndex(groups.parallelStream())
                .map(s->{
                    List<EventWithDate> events = s.getValue().stream()
                            .flatMap(i->{
                                ResultSet rs = session.execute("SELECT " + "events" + " FROM " + cassandra_keyspace_name + "." + table_seq +
                                        " WHERE sequence_id = ? ", i.toString());
                                Row row = rs.one();
                                if (row==null){
                                    return null;
                                }else{
                                    return this.handleRowNaive(row.getList("events", String.class)).stream();
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    Collections.sort(events);
                    return new CombinedTraces(s.getIndex(),events);
                }).collect(Collectors.toList());

    }
    private List<EventWithDate> handleRowNaive(List<String> s){
        return s.parallelStream().map(event->{
            String[] f = event.replace("Event(","").replace(")","")
                    .split(",");
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return new EventWithDate(f[1], dateFormat.parse(f[0]));
            }catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e2){
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }
    private Map<Integer, List<Date>> handleRow(List<String> row, List<Set<Integer>> groups) {

        Set<Integer> ids = groups.parallelStream().flatMap(Collection::stream).collect(Collectors.toSet());
        Map<Integer, List<Date>> results = new HashMap<>();
        row.stream().parallel().map(s -> {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String[] k = s.split("\\(");
            String dates = k[1].substring(0, k[1].length() - 1);
            List<Date> dList = new ArrayList<>();
            for (String d : dates.split(",")) {
                try {
                    dList.add(dateFormat.parse(d));
                } catch (ParseException | NumberFormatException e2) {
                }
            }
            return new TraceWithTimestamps(k[0], dList);
        }).filter(s -> ids.contains(s.event)).forEach(r -> results.put(r.event, r.dates));
        return results;

    }
    private Map<String,List<Lifetime>> evaluateGroups(List<Map<String,EventWithTimestamps>> perGroup, Date start_date,
                                                      Date end_date, List<Event> query,boolean returnAll) {
        return perGroup.stream().parallel()
                .filter(s -> s.size() == query.size())
                .map(s -> {
                    if(returnAll){
                        return evaluateGroupAll(s,start_date,end_date,query);
                    }else{
                        return evaluateGroupOne(s,start_date,end_date,query);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(s->s.left,s->s.right));
    }
    private Map<String,List<Lifetime>> evaluateGroupsNaive(List<CombinedTraces> perGroup, Date start_date, Date end_date,
                                                           List<Event> query, boolean returnAll){
        return perGroup.stream().parallel()
                .map(s->{
                    if(returnAll){
                        return evaluateGroupNaiveAll(s,start_date,end_date,query);
                    }else{
                        return evaluateGroupNaiveOne(s,start_date,end_date,query);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(s->s.left,s->s.right));
    }
    private ImmutablePair<String,List<Lifetime>> evaluateGroupNaiveAll(CombinedTraces group, Date start_date, Date end_date, List<Event> query){
        ImmutablePair<String, List<Lifetime>> results=null;
        List<EventWithDate> foundEvents = new ArrayList<>();
        int k=0;
        for(EventWithDate e:group.getRight()){
            if(e.event.equals(query.get(k).getName()) && e.time.after(start_date) && e.time.before(end_date)){
                foundEvents.add(e);
                k=(k+1)%query.size();
            }
        }
        k=0;
        List<Lifetime> l = new ArrayList<>();
        while(k+ query.size()-1< foundEvents.size()){
            long diff = foundEvents.get(k+ query.size()-1).time.getTime()-foundEvents.get(k).time.getTime();
            l.add(new Lifetime(foundEvents.get(k).time,foundEvents.get(k+ query.size()-1).time,diff));
            k+=query.size();
        }
        if(!l.isEmpty()){
            results= new ImmutablePair<>("Group"+group.id,l);
        }
        return results;

    }
    private ImmutablePair<String,List<Lifetime>> evaluateGroupNaiveOne(CombinedTraces group, Date start_date, Date end_date, List<Event> query){
        ImmutablePair<String, List<Lifetime>> results=null;
        List<EventWithDate> foundEvents = new ArrayList<>();
        int k=0;
        for(EventWithDate e:group.getValue()){
            if(e.event.equals(query.get(k).getName()) && e.time.after(start_date) && e.time.before(end_date)){
                foundEvents.add(e);
                k=(k+1)%query.size();
            }
            if(foundEvents.size()== query.size()){
                break;
            }
        }
        k=0;
        List<Lifetime> l = new ArrayList<>();
        while(k+ query.size()-1< foundEvents.size()){
            long diff = foundEvents.get(k+ query.size()-1).time.getTime()-foundEvents.get(k).time.getTime();
            l.add(new Lifetime(foundEvents.get(k).time,foundEvents.get(k+ query.size()-1).time,diff));
            k+=query.size();
        }
        if(!l.isEmpty()){
            results= new ImmutablePair<>("Group"+group.id,l);
        }
        return results;

    }
    private ImmutablePair<String, List<Lifetime>> evaluateGroupAll(Map<String,EventWithTimestamps> group, Date start_date, Date end_date, List<Event> query) {
        List<Date> foundEvents = new ArrayList<>();
        ImmutablePair<String, List<Lifetime>> results=null;
        int group_id=-1;
        String eString;
        boolean added = true;
        int i=0;
        while(added){
            added=false;
            eString = query.get(i).getName();
            EventWithTimestamps evs = group.getOrDefault(eString,null);
            if (evs == null) return results;
            if (foundEvents.isEmpty()) {
                group_id=evs.getGroupId();
                for (Date d : evs.getDates()) {
                    if (d.after(start_date) && d.before(end_date)) {
                        foundEvents.add(evs.getDates().get(0));
                        added=true;
                        break;
                    }
                }
            } else {
                for (Date d : evs.getDates()) {
                    if (d.after(foundEvents.get(foundEvents.size()-1))) {// if the next one is before the previous one
                        if (d.after(start_date) && d.before(end_date)) {
                            foundEvents.add( d);
                            added=true;
                            break;
                        }
                    }
                }
            }
            i=(i+1) % query.size();
        }
        int k=0;
        List<Lifetime> l = new ArrayList<>();
        while(k+ query.size()-1< foundEvents.size()){
            long diff = foundEvents.get(k+ query.size()-1).getTime()-foundEvents.get(k).getTime();
            l.add(new Lifetime(foundEvents.get(k),foundEvents.get(k+ query.size()-1),diff));
            k+=query.size();
        }
        if(!l.isEmpty()){
            results= new ImmutablePair<>("Group"+group_id,l);
        }
        return results;
    }
    private ImmutablePair<String, List<Lifetime>> evaluateGroupOne(Map<String,EventWithTimestamps> group, Date start_date, Date end_date, List<Event> query) {
        List<Date> foundEvents = new ArrayList<>();
        ImmutablePair<String, List<Lifetime>> results=null;
        int group_id=-1;
        String eString;
        boolean added = true;
        int i=0;
        while(added){
            added=false;
            eString = query.get(i).getName();
            EventWithTimestamps evs = group.getOrDefault(eString,null);
            if (evs == null) return results;
            if (foundEvents.isEmpty()) {
                group_id=evs.getGroupId();
                for (Date d : evs.getDates()) {
                    if (d.after(start_date) && d.before(end_date)) {
                        foundEvents.add(evs.getDates().get(0));
                        added=true;
                        break;
                    }
                }
            } else {
                for (Date d : evs.getDates()) {
                    if (d.after(foundEvents.get(foundEvents.size()-1))) {// if the next one is before the previous one
                        if (d.after(start_date) && d.before(end_date)) {
                            foundEvents.add( d);
                            added=true;
                            break;
                        }
                    }
                }
            }
            i=(i+1) % query.size();
            if(foundEvents.size()== query.size()){
                break;
            }
        }
        int k=0;
        List<Lifetime> l = new ArrayList<>();
        while(k+ query.size()-1< foundEvents.size()){
            long diff = foundEvents.get(k+ query.size()-1).getTime()-foundEvents.get(k).getTime();
            l.add(new Lifetime(foundEvents.get(k),foundEvents.get(k+ query.size()-1),diff));
            k+=query.size();
        }
        if(!l.isEmpty()){
            results= new ImmutablePair<>("Group"+group_id,l);
        }
        return results;
    }


}
