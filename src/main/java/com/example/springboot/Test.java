package com.example.springboot;

import java.util.Date;

public class Test {

    public static void main(String[] args) throws InterruptedException {

        Date d1=new Date();

        Thread.sleep(10000);

        Date d2=new Date();




      System.out.println( d2.getTime()-d1.getTime());

       // System.out.println(d);


    }
}
