package com.example.springboot;

import java.util.*;

public class RequestData {

    //List<String> attributeList=new ArrayList<>(Arrays.asList("Gender","Age","Weight","Height"));
    private String reqType;

    private List<EachReq> eachReq;

    public String getReqType() {
        return reqType;
    }

    public void setReqType(String reqType) {
        this.reqType = reqType;
    }

    public List<EachReq> getEachReq() {
        return eachReq;
    }

    public void setEachReq(List<EachReq> eachReq) {
        this.eachReq = eachReq;
    }

    //Map<String,String> attributeList=new HashMap<>();


}
