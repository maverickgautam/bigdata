package com.big.data.java.samples;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Lamda {

    public static void main(String[] args) {

        List<Integer> integerList = Arrays.asList(10, 20, 30, 40);

        //Traditional For loop
        for (int i = 0; i < integerList.size(); i++) {
            System.out.println("Traiditional for loop value of i is : " + integerList.get(i));
        }

        //Advance for loop
        for (int value : integerList) {
            System.out.println("Advance for loop value of arrayLis is : " + value);
        }

        //Internal iterator using Streams Annonymous class
        integerList.stream().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println("Stream + anonymous class the value is " + integer);
            }
        });

        // Lamdda with braces
        integerList.stream().forEach(integer -> {
            System.out.println("lamda with braces value is " + integer);
        });

        // Lamdda without braces
        integerList.stream().forEach(integer -> System.out.println("lamda without braces value is " + integer));

        //Calling function directly in lamda
        integerList.stream().forEach(System.out::println);

    }

}
