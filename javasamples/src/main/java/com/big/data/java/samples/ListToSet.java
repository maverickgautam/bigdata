package com.big.data.java.samples;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by kunalgautam on 18.03.17.
 */
public class ListToSet {

    public static void main(String[] args) {

        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5);

        Set<Integer> integerSet = integerList.stream()
                                             .collect(Collectors.toSet());

        integerSet.forEach(System.out::println);







        // To obtain a specific Implementation of Set.

        Set<Integer> set = integerList.stream().collect(new Supplier<Set<Integer>>() {
            @Override
            public Set<Integer> get() {
                return new LinkedHashSet<>();
            }
        }, new BiConsumer<Set<Integer>, Integer>() {
            @Override
            public void accept(Set<Integer> integers, Integer integer) {
                integers.add(integer);

            }
        }, new BiConsumer<Set<Integer>, Set<Integer>>() {
            @Override
            public void accept(Set<Integer> integers, Set<Integer> integers2) {
                integers.addAll(integers2);

            }
        });

        set.stream().forEach(System.out::println);

    }

}
