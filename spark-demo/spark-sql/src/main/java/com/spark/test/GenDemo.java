package com.spark.test;


import com.spark.bean.Gen;

public class GenDemo {
    public static void main(String[] args) {
        //定义泛型类Gen的一个Integer版本
        Gen<Integer> intOb = new Gen<Integer>(88);
        intOb.showTyep();
        int i = intOb.getOb();
        System.out.println("value= " + i);
        System.out.println("----------------------------------");
        //定义泛型类Gen的一个String版本
        Gen<String> strOb = new Gen<String>("Hello Gen!");
        strOb.showTyep();
        String s = strOb.getOb();
        System.out.println("value= " + s);
    }
}