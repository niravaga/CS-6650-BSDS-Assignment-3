package restfulAPI;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;

/**
 * REST Web Service
 *
 * @author mkuchtiak
 */
@Stateless
@Path("/ping")
public class PingTest {

    /**
     * Retrieves representation of an instance of helloworld.PublisherResource
     *
     * @return an instance of java.lang.String
     */
    @GET
    @Produces("text/plain")
    public String getGreeting() {

        return "Hello World";
    }
}
