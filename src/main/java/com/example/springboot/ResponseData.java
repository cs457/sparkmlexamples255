package com.example.springboot;

import java.util.Date;

public class ResponseData {

    String timeTook;

    Date modelCreatedTime=null;

    public String getTimeTook() {
        return timeTook;
    }

    public void setTimeTook(String timeTook) {
        this.timeTook = timeTook;
    }

    public Date getModelCreatedTime() {
        return modelCreatedTime;
    }

    public void setModelCreatedTime(Date modelCreatedTime) {
        this.modelCreatedTime = modelCreatedTime;
    }
}
