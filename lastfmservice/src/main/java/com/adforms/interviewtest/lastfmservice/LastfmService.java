package com.adforms.interviewtest.lastfmservice;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.adforms.interviewtest.analysis.Lastfm;
import com.adforms.interviewtest.commons.*;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;

/**
 * Simple REST service wrapping up around lastfm API
 */
@Path("/lastfm")
public class LastfmService {
    // a static lastfm API instance initialized with the event log file, ser profile file, Spark master
    static Lastfm api = new Lastfm("dataset/2user.tsv", "dataset/userid-profile.tsv", "local[*]");

    @GET
    @Path("/uniqueUsers")
    public Response uniqueUsers() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String[] users = api.uniqueUsers();
        return Response.status(200).entity(mapper.writeValueAsString(users)).build();
    }

    @GET
    @Path("/topUsers/{count}")
    public Response topUsers(@PathParam("count") int count) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TopUser[] users = api.topUsers(count);
        return Response.status(200).entity(mapper.writeValueAsString(users)).build();
    }

    @GET
    @Path("/topSessions/{count}")
    public Response topSessions(@PathParam("count") int count) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TopSessionSong[] sessions = api.topSessions2(count);
        return Response.status(200).entity(mapper.writeValueAsString(sessions)).build();
    }

    @GET
    @Path("topSongs/{count}")
    public Response topSongs(@PathParam("count") int count) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TopSong[] songs = api.topSongs(count);
        return Response.status(200).entity(mapper.writeValueAsString(songs)).build();
    }

    @GET
    @Path("/recommend/{user}/{count}")
    public Response recommend(@PathParam("user") String  user, @PathParam("count") int  count) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Recommendations reco = api.recommend(user, count);
        return Response.status(200).entity(mapper.writeValueAsString(reco)).build();
    }

    @GET
    @Path("/predictNextSong/{user}")
    public Response predictNextSong(@PathParam("user") String  user) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Song song = api.predictNextSong(user);
        return Response.status(200).entity(mapper.writeValueAsString(song)).build();
    }

    @GET
    @Path("/predictNextPlayTime/{user}")
    public Response predictNextPlayTime(@PathParam("user") String  user) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Date date = api.predictNextPlayTime(user);
        return Response.status(200).entity(mapper.writeValueAsString(date.toString())).build();
    }
}