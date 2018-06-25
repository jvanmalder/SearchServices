package org.alfresco.solr.sql;

import static org.alfresco.solr.sql.SolrSchemaUtil.extractPredicates;

import java.util.Collections;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

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
}
