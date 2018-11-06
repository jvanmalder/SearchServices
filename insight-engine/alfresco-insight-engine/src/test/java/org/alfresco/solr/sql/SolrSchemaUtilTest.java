package org.alfresco.solr.sql;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.alfresco.solr.sql.SolrSchemaUtil.extractPredicates;

public class SolrSchemaUtilTest
{
    @Test
    public void testPredicateExtractor()
    {
        Set<String> predicates = extractPredicates("select * from  alfresco");
        Assert.assertEquals(Collections.EMPTY_SET, predicates);
        predicates = extractPredicates("SELECT * FROM  ALFRESCO");
        Assert.assertEquals(Collections.EMPTY_SET, predicates);
        predicates = extractPredicates("");
        Assert.assertEquals(Collections.EMPTY_SET, predicates);
        predicates = extractPredicates("select * from  alfresco where `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        predicates = extractPredicates("select * from  alfresco WHERE `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where cm_content = 'rock'");
        Assert.assertTrue("cm_content", predicates.contains("cm_content"));
        Assert.assertEquals(1, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(1, predicates.size());
        //Handle invalid predicate.
        predicates = extractPredicates("select * from  alfresco Where 'DBID' = '2'");
        Assert.assertEquals(0, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' AND PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' and PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' And PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' OR PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' or PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select * from  alfresco Where DBID = '2' Or PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select DBID from alfresco where TYPE = 'content' AND NOT TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select DBID from alfresco where TYPE = 'content' AND not TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
        predicates = extractPredicates("select DBID from alfresco where TYPE = 'content' AND Not TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
    }

    @Test 
    public void predicateExtraction_singlePredicateStrictInequalityOperand_shouldExtractCorrectFieldName()
    {
        Set<String> predicates = extractPredicates("select * from alfresco where customField1 != 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = extractPredicates("select * from alfresco where customField1 <> 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = extractPredicates("select * from alfresco where customField1 ~= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = extractPredicates("select * from alfresco where customField1 < 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = extractPredicates("select * from alfresco where customField1 > 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());
    }

    @Test 
    public void predicateExtraction_multiPredicateStrictInequalityOperand_shouldExtractCorrectFieldNames()
    {
        Set<String> predicates = extractPredicates(
            "select * from alfresco where customField1 != 3 AND customField2 <> 3 AND customField3 ~= 3 AND customField4 > 3 AND customField5 < 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertTrue("customField2", predicates.contains("customField2"));
        Assert.assertTrue("customField3", predicates.contains("customField3"));
        Assert.assertTrue("customField4", predicates.contains("customField4"));
        Assert.assertTrue("customField5", predicates.contains("customField5"));

        Assert.assertEquals(5, predicates.size());
    }

    @Test 
    public void predicateExtraction_singlePredicateInequalityOperand_shouldExtractCorrectFieldName()
    {
        Set<String> predicates = extractPredicates("select * from alfresco where customField1 >= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = extractPredicates("select * from alfresco where customField1 <= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());
    }

    @Test 
    public void predicateExtraction_multiPredicateInequalityOperand_shouldExtractCorrectFieldNames()
    {
        Set<String> predicates = extractPredicates(
            "select * from alfresco where customField1 >= 3 AND customField2 <= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertTrue("customField2", predicates.contains("customField2"));

        Assert.assertEquals(2, predicates.size());
    }
    
    @Test
    public void lockOwnerFieldExists()
    {
        Assert.assertFalse(SolrSchema.lockOwnerFieldExists(""));
        Assert.assertFalse(SolrSchema.lockOwnerFieldExists(null));
        Assert.assertFalse(SolrSchema.lockOwnerFieldExists("Test"));
        Assert.assertFalse(SolrSchema.lockOwnerFieldExists("cm_LockOwner"));
        Assert.assertTrue(SolrSchema.lockOwnerFieldExists("cm_lockOwner"));
        Assert.assertTrue(SolrSchema.lockOwnerFieldExists("cm:lockOwner"));
    }
}
