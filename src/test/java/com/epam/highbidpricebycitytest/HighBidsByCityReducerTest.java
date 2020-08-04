package com.epam.highbidpricebycitytest;

import com.epam.highbidpricebycity.HighBidsByCityReducer;
import com.epam.highbidpricebycity.comparable.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HighBidsByCityReducerTest {
    private ReduceDriver<TextPair, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        HighBidsByCityReducer reducer = new HighBidsByCityReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> countValues = new ArrayList<>();
        countValues.add(new IntWritable(3));
        countValues.add(new IntWritable(1));
        reduceDriver.withInput(new TextPair("test city", "test OS"), countValues);
        reduceDriver.withOutput(new Text("test city"), new IntWritable(4));
        reduceDriver.runTest();
    }
}
