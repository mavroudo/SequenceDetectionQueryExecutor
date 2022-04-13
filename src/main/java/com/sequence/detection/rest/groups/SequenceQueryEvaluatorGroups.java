package com.sequence.detection.rest.groups;

import com.codepoetics.protonpack.StreamUtils;
import com.datastax.driver.core.*;
import com.sequence.detection.rest.model.AugmentedDetail;
import com.sequence.detection.rest.model.Event;
import com.sequence.detection.rest.model.Lifetime;
import com.sequence.detection.rest.model.Sequence;
import com.sequence.detection.rest.query.SequenceQueryHandler;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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

    private class EventWithTimestamps{
        private String event;
        private List<Date> dates;

        private Integer groupId;
        public EventWithTimestamps(String event, List<Date> dates, int groupId) {
            this.event = event;
            this.dates = dates;
            Collections.sort(this.dates);
            this.groupId=groupId;
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

    public Map<String, Lifetime> detect(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> queryDetails, String table_name, List<Set<Integer>> groups) {
        HashMap<String, Lifetime> results = new HashMap<>();
        int l = table_name.split("_").length;
        String table_one = String.join("_", Arrays.copyOfRange(table_name.split("_"), 0, l - 1)) + "_one";
        List<List<EventWithTimestamps>> perEvent = this.queryEvent(query.getList(),queryDetails,table_one);
        System.out.println(perEvent);



        return results;
    }

    private List<List<EventWithTimestamps>> queryEvent(List<Event> events, Map<Integer, List<AugmentedDetail>> queryDetails, String table_one) {
        List<List<EventWithTimestamps>> perEvent =  events
                .stream()
                .parallel()
                .map(s->{
            ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + table_one +
                    " WHERE event_name = ? ", s.getName());
            Row row = rs.one();
            Map<Integer,List<Date>> dates = this.handleRow(row.getList("sequences", String.class),groups);
            List<EventWithTimestamps> e = StreamUtils.zipWithIndex(groups.stream().parallel())
                    .map(g->{
                        List<Date> ds = g.getValue().stream().map(x->dates.getOrDefault(x,new ArrayList<>())).reduce(new ArrayList<>(),(a,b)->{

                            a.addAll(b);
                            return a;
                        });
                        return new EventWithTimestamps(s.getName(),ds, (int) g.getIndex());
                    })
                    .collect(Collectors.toList());
            return e;
        }).collect(Collectors.toList());
        return StreamUtils.zipWithIndex(groups.stream().parallel())
                .map(g-> perEvent.stream()
                        .flatMap(Collection::stream)
                        .filter(x->x.groupId==g.getIndex())
                        .collect(Collectors.toList())).collect(Collectors.toList());
    }

    private Map<Integer, List<Date>> handleRow(List<String> row, List<Set<Integer>> groups){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<Integer> ids = groups.parallelStream().flatMap(Collection::stream).collect(Collectors.toSet());
        Map<Integer, List<Date>> results = new HashMap<>();
        row.stream().map(s -> {
            String[] k = s.split("\\(");
            String dates = k[1].substring(0,k[1].length()-1);
            List<Date> dList = new ArrayList<>();
            for(String d :dates.split(",")){
                try{
                    dList.add(dateFormat.parse(d));
                }catch (ParseException | NumberFormatException  e2){}
            }
            return new TraceWithTimestamps(k[0],dList);
        }).filter(s-> ids.contains(s.event)).forEach(r-> results.put(r.event,r.dates));
        return results;

    }

//    private void evaluateGroups(List<List<EventWithTimestamps>> perEvent,Date start_date, Date end_date){
//        StreamUtils.zipWithIndex(groups.stream().parallel())
//                .map(g->{
//                    List<EventWithTimestamps> lists = perEvent.stream()
//                            .flatMap(Collection::stream)
//                            .filter(x->x.groupId==g.getIndex())
//                            .collect(Collectors.toList());
//                });
//    }


}
