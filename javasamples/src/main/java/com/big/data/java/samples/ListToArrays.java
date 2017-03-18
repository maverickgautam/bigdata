package com.big.data.java.samples;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kunalgautam on 18.03.17.
 */
public class ListToArrays {

    public static void main(String[] args) {

        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5);

        // ObjectTyped aray
        Object[] objArray = integerList.stream().toArray();
        Arrays.stream(objArray).forEach(System.out::println);

        //RequiredTyped Array
        Integer[] intArray = integerList.stream().toArray(Integer[]::new);
        Arrays.stream(intArray).forEach(System.out::println);

        ;

    }
}
