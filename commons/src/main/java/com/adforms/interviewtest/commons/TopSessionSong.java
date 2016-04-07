package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * Encapsulates a particular song from a play session
 */
public class TopSessionSong implements Serializable {
    private Long sessionId;
    private Long sessionLength;
    private String user;
    private String artist;
    private String song;

    public TopSessionSong(Long sessionId, Long sessionLength, String user, String artist, String song){
        this.sessionId = sessionId;
        this.sessionLength = sessionLength;
        this.user = user;
        this.artist = artist;
        this.song = song;
    }

    public Long getSessionId() {
        return sessionId;
    }

    public Long getSessionLength() {
        return sessionLength;
    }

    public String getUser() {
        return user;
    }

    public String getArtist() {
        return artist;
    }

    public String getSong() {
        return song;
    }
}
