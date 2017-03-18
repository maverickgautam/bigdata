package com.big.data.java.samples;

import java.util.Arrays;
import java.util.List;

public class LoopFor {

    public void loop1() {

        // For loop consists of three part
        // Initialization , conditional check ,  expreseeion evaluation (increment decrement)

        for (int i = 0; i <= 3; i++) {
            System.out.println("value of i inside LOOP1 method is : " + i);
        }

        // System.out.println( i);  error i can be accessed only within the for loop

    }

    public void loop2() {

        // For loop consists of three part
        // Initialization , conditional check (boolean) ,  expreseeion evaluation (increment decrement)

        int i = 0;

        for (; i <= 3; i++) {
            System.out.println("value of i inside LOOP2 method is : " + i);
        }

        System.out.println("value of i in Looop2 outside for loop  is " + i);

    }

    public void loop3() {

        for (; true; ) {
            // infinite while loop in Java
        }

        // for(;1;)  1 is not taken as true .  while accepts only boolean consition
    }

    private int getConditionSize() {
        System.out.println("getConditionSize is being called");
        return 3;
    }

    public void loop4() {

        for (int i = 0; i <= getConditionSize(); i++) {
            System.out.println("value of i inside LOOP4 method is : " + i);
        }

    }

    public void loop5() {

        List<String> names = Arrays.asList("Maverick", "Rock");

        for (String name : names) {
            System.out.println("In LOOP5 name is " + name);
        }

        for (int i = 0; i < names.size(); i++) {
            System.out.println("In LOOP5 iterating through traditional way is " + names.get(i));
        }

    }

    public static void main(String[] args) {

        LoopFor loopFor = new LoopFor();
        loopFor.loop1();
        loopFor.loop2();
        //loopFor.loop3(); Infinite Loop
        loopFor.loop4();
        loopFor.loop5();
    }

}

