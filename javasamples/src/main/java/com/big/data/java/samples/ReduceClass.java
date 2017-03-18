package com.big.data.java.samples;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Created by kunalgautam on 18.03.17.
 */
public class ReduceClass {

    public static void main(String[] args) {

        List<Integer> integerList = Arrays.asList(1, 2, 3, 4);

        // Sum of the integerList by imperative stype of coding is
        int sum = 0;
        for (int value : integerList) {

            sum += value;
        }
        System.out.println("The sum of the integerList is : " + sum);

        // Lets do  the same stuff by reduce
        Optional<Integer> total = integerList.stream()
                                             // first integer1 the sum from the previous call in the internal iterator
                                             .reduce((integer1, integer2) -> integer1 + integer2);

        System.out.println("Using reduce in Lambda sum is " + total.get());

        // Lets understand what integer1 reflects
        Optional<Integer> total1 = integerList.stream()
                                              // first integer1 the sum from the previous call in the internal iterator
                                              .reduce((integer1, integer2) -> {
                                                  System.out.println("value of integer1  is  " + integer1 + "  Integer2 is  " + integer2);
                                                  return integer1 + integer2;
                                              });

        System.out.println("Using reduce in Lambda sum is " + total1.get());

        // What if we want the base Sum not be 0 but 100.

        // Lets do  the same stuff by reduce
        Integer total3 = integerList.stream()
                                    // first integer1 the sum from the previous call in the internal iterator
                                    .reduce(100, (integer1, integer2) ->
                                            {
                                                System.out.println("value of integer1  is " + integer1 + "  Integer2 is      " + integer2);
                                                return integer1 + integer2;
                                            }
                                    );

        System.out.println("Using reduce in Lambda with base = 100 sum is " + total3);

        // Lets do  the same stuff by reduce
        Integer total4 = integerList.parallelStream()
                                    // first integer1 the sum from the previous call in the internal iterator
                                    .reduce(100,

                                            (integer1, integer2) -> {

                                                System.out.println("value inside accumulate of integer1  is " + integer1 + "  Integer2 is      " +
                                                                           integer2);
                                                return integer1 + integer2;
                                            },

                                            (integer, integer2) -> {

                                                System.out.println("value inside combiner of integer1  is " + integer + "  Integer2 is      " +
                                                                           integer2);
                                                return (integer > integer2 ? integer : integer2);
                                            }
                                    );

        System.out.println("Using reduce in Lambda with base = 100 sum is " + total4);

    }

}
