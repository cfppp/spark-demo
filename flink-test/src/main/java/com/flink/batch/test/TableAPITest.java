package com.flink.batch.test;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;


public class TableAPITest {

    public static void main (String[] args){



        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

// register Orders table in table environment
// ...

// specify table program
        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));

// print
        counts.execute().print();

    }

}
