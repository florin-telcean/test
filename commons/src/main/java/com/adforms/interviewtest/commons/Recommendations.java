package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * The recommendations for a given user as lists of recommended artists and recommended songs
 */
public class Recommendations implements Serializable {
    private Artist[] artists;
    private Song[] songs;

    public Recommendations( Artist[] artists, Song[] songs){
        this.artists = artists;
        this.songs = songs;
    }

    public Artist[] getArtists() {
        return artists;
    }

    public Song[] getSongs() {
        return songs;
    }
}
