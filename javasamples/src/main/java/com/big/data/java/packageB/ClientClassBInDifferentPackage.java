package com.big.data.java.packageB;

import com.big.data.java.packageA.SuperClassA;

public class ClientClassBInDifferentPackage {

    public static void main(String[] args) {

        // accessing superClassA from a client class in a different package as that of the superClass
        SuperClassA superClassA = new SuperClassA();
        System.out.println("Parent class method call output is :  " + superClassA.getState());

    }
}
