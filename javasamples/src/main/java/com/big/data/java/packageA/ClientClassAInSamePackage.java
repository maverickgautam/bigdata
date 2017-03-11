package com.big.data.java.packageA;

public class ClientClassAInSamePackage {

    public static void main(String[] args) {

        // accessing superClassA from a client class in the same package as that of the superClass
        SuperClassA superClassA = new SuperClassA();
        System.out.println("Parent class method call output is :  " + superClassA.getState());

    }
}
