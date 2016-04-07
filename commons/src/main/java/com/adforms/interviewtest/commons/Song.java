package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * Encapsulates a Song by Artist Id, Artist Name, Song Id, Song Name
 */
public class Song implements Serializable{
    private String artist;
    private String artistId;
    private String song;
    private String songId;

    public Song(String artistId, String artist, String songId, String song){
        this.artistId = artistId;
        this.artist = artist;
        this.songId = songId;
        this.song = song;
    }

    public String getArtist() {
        return artist;
    }

    public String getArtistId() {
        return artistId;
    }

    public String getSong() {
        return song;
    }

    public String getSongId() {
        return songId;
    }
}
