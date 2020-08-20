package com.fa.funnel.rest.query;

import com.fa.funnel.rest.model.Detail;
import com.fa.funnel.rest.model.Funnel;
import com.fa.funnel.rest.model.Name;
import com.fa.funnel.rest.model.Step;
import com.fa.funnel.rest.util.Utilities;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ResponseBuilderTest
{
    static List<List<Step>> results;
    static List<List<Step>> resultsSingleOR;
    static List<List<Step>> resultsDoubleOR;
    
    @BeforeClass
    public static void setUp()
    {
        // First step
        Step step1 = new Step();
        
        List<Name> names1 = new ArrayList<Name>();
        Name name1 = new Name();
        name1.setLogName("A");
        names1.add(name1);
        step1.setMatchName(names1);
        List<Detail> details1 = new ArrayList<Detail>();
        Detail detail1 = new Detail();
        detail1.setKey("randomKey");
        detail1.setOperator("=");
        detail1.setValue("randomValue");
        details1.add(detail1);
        step1.setMatchDetails(details1);
        
        // Second step
        Step step2 = new Step();
 
        List<Name> names2 = new ArrayList<Name>();
        Name name2 = new Name();
        name2.setLogName("B");
        names2.add(name2);
        
        step2.setMatchName(names2);
        
        // Third step
        Step step3 = new Step();
        
        List<Name> names3 = new ArrayList<Name>();
        Name name3 = new Name();
        name3.setLogName("C");
        names3.add(name3);
        
        step3.setMatchName(names3);
        
        // Fourth step
        Step step4 = new Step();

        List<Name> names4 = new ArrayList<Name>();
        Name name4 = new Name();
        name4.setLogName("D");
        names4.add(name4);
        step4.setMatchName(names4);
        List<Detail> details4 = new ArrayList<Detail>();
        Detail detail4 = new Detail();
        detail4.setKey("randomKey4");
        detail4.setOperator("=4");
        detail4.setValue("randomValue4");
        details4.add(detail4);
        step4.setMatchDetails(details4);
        
        // Create step list
        List<Step> steps = new ArrayList<Step>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        
        Funnel funnel = new Funnel();
        funnel.setSteps(steps);
        ResponseBuilder rb = new ResponseBuilder(null, null, null, null, funnel, "1970-01-01", Utilities.getToday());
        
        results = rb.simplifySequences(steps);
        
        Name nameOR = new Name();
        nameOR.setLogName("E");
        steps.get(1).getMatchName().add(nameOR);
        resultsSingleOR = rb.simplifySequences(steps);
        
        Name nameOR2 = new Name();
        nameOR2.setLogName("F");
        steps.get(3).getMatchName().add(nameOR2);
        resultsDoubleOR = rb.simplifySequences(steps);
    }
    
    @Test
    public void testSimplifySequence_NoOREvents()
    {   
//        System.out.println(results);
        assertTrue(results.size()==1);
    }
    
    @Test
    public void testSimplifySequence_SingleOREvent()
    {   
//        System.out.println(resultsSingleOR);
        assertTrue(resultsSingleOR.size()==2);
    }
    
    @Test
    public void testSimplifySequence_DoubleOREvents()
    {   
//        System.out.println(resultsDoubleOR);
        assertTrue(resultsDoubleOR.size()==4);
    }
}
