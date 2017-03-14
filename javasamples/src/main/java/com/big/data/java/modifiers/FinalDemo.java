package com.big.data.java.modifiers;

import java.util.HashMap;
import java.util.Map;

final class ParentClass {
    int a;
};

// public class ChildClass extends ParentClass not possible as parentClass is final



public class FinalDemo {

    // value set as part of initialization
    final int a = 10;

    // value needs to be set in constructor
    final Map<String, String> mapReference;

    public FinalDemo() {

        //a = 20; not possible as 10 has been set in the intialization
        mapReference = new HashMap<>();
    }

    public void changeMap() {

        // mapReference = new HashMap<>(); not possible as mapReference is final and has been  initialized in constructor.


        // Object new HashMap<>() can change its state but reference needs to keep pointig at mapReference = new HashMap<>();
        mapReference.put("1", "1");


        //a = 20; primitive type cannot change its value as it is final
    }

}
