package org.alfresco.solr.sql;

import org.junit.Assert;
import org.junit.Test;

public class SolrSchemaUtilTest
{
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
