package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * Encapsulates a song by Id and its play count
 */
public class TopSong implements Serializable{
    private String songId;
    private long playCount;

    public TopSong(String id, long count){
        this.songId = id;
        this.playCount = count;
    }

    public String getSongId() {
        return songId;
    }

    public long getPlayCount() {
        return playCount;
    }

    public String toString(){
        return (String.format("Song id: %s, Play count: %d", songId, playCount));
    }
}
