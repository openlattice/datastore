package com.dataloom.datastore.util;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import com.auth0.spring.security.api.Auth0AuthenticationEntryPoint;
import com.auth0.spring.security.api.Auth0TokenException;
import com.dataloom.organizations.roles.exceptions.TokenRefreshException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.datastore.util.ErrorsDTO;

/**
 * Returns json error messages. The format of error messages here has to be consistent with
 * {@link DataStoreExceptionHandler}. Modified from {@link Auth0AuthenticationEntryPoint}
 * 
 * @author Ho Chung Siu
 *
 */
public class RefreshTokenAuthenticationEntryPoint implements AuthenticationEntryPoint {
    private ObjectMapper mapper;

    public RefreshTokenAuthenticationEntryPoint( ObjectMapper mapper ) {
        this.mapper = mapper;
    }

    @Override
    public void commence(
            HttpServletRequest request,
            HttpServletResponse response,
            AuthenticationException authException ) throws IOException, ServletException {
        response.setContentType( "application/json" );
        final PrintWriter writer = response.getWriter();
        if ( isPreflight( request ) ) {
            response.setStatus( HttpServletResponse.SC_NO_CONTENT );
        } else if ( authException instanceof TokenRefreshException ) {
            response.setStatus( HttpServletResponse.SC_UNAUTHORIZED );
            writer.println(
                    mapper.writeValueAsString( new ErrorsDTO( "TokenRefreshException", authException.getMessage() ) ) );

        } else if ( authException instanceof Auth0TokenException ) {
            response.setStatus( HttpServletResponse.SC_UNAUTHORIZED );
            writer.println(
                    mapper.writeValueAsString( new ErrorsDTO( "Auth0TokenException", authException.getMessage() ) ) );
        } else {
            response.setStatus( HttpServletResponse.SC_FORBIDDEN );
            writer.println(
                    mapper.writeValueAsString( new ErrorsDTO( "ForbiddenException", authException.getMessage() ) ) );
        }
    }

    /**
     * Checks if this is a X-domain pre-flight request.
     */
    private boolean isPreflight( HttpServletRequest request ) {
        return "OPTIONS".equals( request.getMethod() );
    }
}
