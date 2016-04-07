package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * Encapsulates an Artist by Id, and Name
 */
public class Artist implements Serializable {
    private String artist;
    private String artistId;

    public Artist(String artistId, String artist){
        this.artistId = artistId;
        this.artist = artist;
    }

    public String getArtist() {
        return artist;
    }

    public String getArtistId() {
        return artistId;
    }
}
