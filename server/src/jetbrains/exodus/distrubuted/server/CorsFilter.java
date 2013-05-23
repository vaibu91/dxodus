package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CorsFilter implements ContainerResponseFilter {

    private final List<String> ALLOWED_METHODS = Arrays.asList("OPTIONS", "GET", "POST", "PUT", "DELETE");

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        System.out.println("filter " + request.getMethod());

        if (request.getHeaderValue("Origin") != null) {
            final MultivaluedMap<String, Object> headers = response.getHttpHeaders();
            headers.add("Access-Control-Allow-Origin", "http://localhost:9000");
            headers.add("Access-Control-Expose-Headers", "X-Cache-Date");
            headers.add("Access-Control-Expose-Headers", "X-Atmosphere-tracking-id");
            headers.add("Access-Control-Allow-Credentials", Boolean.TRUE.toString());
        }

        if ("OPTIONS".equals(request.getMethod())) {
            final MultivaluedMap<String, Object> headers = response.getHttpHeaders();
            for (String method : ALLOWED_METHODS) {
                headers.add("Access-Control-Allow-Methods", method);
            }
            headers.add("Access-Control-Allow-Headers",
                    "accept, origin, Content-Type, X-Atmosphere-Framework, X-Cache-Date, X-Atmosphere-tracking-id, X-Atmosphere-Transport, authorization");
            headers.add("Access-Control-Max-Age", "-1");
//            res.addHeader("Access-Control-Allow-Credentials", Boolean.TRUE.toString());
        }
        return response;
    }

}
