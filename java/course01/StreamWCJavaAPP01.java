package com.flink.java.course01;

import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Parameter;

public class StreamWCJavaAPP01 {
    public static void main(String[] args) {
        int port =0;
        try{
            ParameterTool tool=ParameterTool.fromArgs(args);
            tool.getInt("port");
        }catch (Exception e){
            System.err.println("端口未设置，使用默认端口7777");
            port=7777;
        }

    }
}
