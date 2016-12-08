package org.alfresco.rest.auth;

import org.alfresco.rest.RestTest;
import org.alfresco.rest.exception.JsonToModelConversionException;
import org.alfresco.rest.model.RestTicketBodyModel;
import org.alfresco.rest.model.RestTicketModel;
import org.alfresco.utility.model.TestGroup;
import org.alfresco.utility.testrail.ExecutionType;
import org.alfresco.utility.testrail.annotation.TestRail;
import org.springframework.http.HttpStatus;
import org.testng.annotations.Test;

public class AuthTests extends RestTest
{
    @TestRail(section = { TestGroup.REST_API }, executionType = ExecutionType.SANITY, description = "Verify TICKET is returned on admin user")
    @Test
    public void adminShouldGetTicketBody() throws JsonToModelConversionException, Exception
    {
        RestTicketBodyModel ticketBody = new RestTicketBodyModel();
        ticketBody.setUserId("admin");
        ticketBody.setPassword("admin");
        
        RestTicketModel ticketReturned = restClient.withAuthAPI().createTicket(ticketBody);
        
        restClient.assertStatusCodeIs(HttpStatus.CREATED);
        ticketReturned.assertThat().field("id").contains("TICKET_");       
    }

}