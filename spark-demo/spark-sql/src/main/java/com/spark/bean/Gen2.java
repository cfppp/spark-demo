package com.spark.bean;

public class Gen2 {
    private Object ob;

    //定义一个通用类型成员
    public Gen2(Object ob) {
        this.ob = ob;
    }

    public Object getOb() {
        return ob;
    }

    public void setOb(Object ob) {
        this.ob = ob;
    }

    public void showTyep() {
        System.out.println("T的实际类型是: " + ob.getClass().getName());
    }
}

