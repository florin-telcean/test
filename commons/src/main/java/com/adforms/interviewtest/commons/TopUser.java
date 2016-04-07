package com.adforms.interviewtest.commons;

import java.io.Serializable;

/**
 * Encapsulates an user and the count of played songs
 */
public class TopUser implements Serializable {
    private String userName;
    private long songCount;

    public TopUser(String user, long count){
        this.userName = user;
        this.songCount = count;
    }

    public String getUserName() {
        return userName;
    }

    public long getSongCount() {
        return songCount;
    }

}
