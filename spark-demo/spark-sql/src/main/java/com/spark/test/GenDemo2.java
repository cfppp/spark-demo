package com.spark.test;


import com.spark.bean.Gen2;

public class GenDemo2 {
    public static void main(String[] args) {
        //定义类Gen2的一个Integer版本
        Gen2 intOb = new Gen2(new Integer(88));
        intOb.showTyep();
        int i = (Integer) intOb.getOb();
        System.out.println("value= " + i);
        System.out.println("----------------------------------");
        //定义类Gen2的一个String版本
        Gen2 strOb = new Gen2("Hello Gen!");
        strOb.showTyep();
        String s = (String) strOb.getOb();
        System.out.println("value= " + s);
    }
}