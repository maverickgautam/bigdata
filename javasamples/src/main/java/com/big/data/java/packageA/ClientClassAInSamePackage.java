package com.big.data.java.packageA;

public class ClientClassAInSamePackage {

    public static void main(String[] args) {

        // accessing superClassA from a client class in the same package as that of the superClass
        SuperClassA superClassA = new SuperClassA();
        // Private is not accessible. 

        System.out.println("Parent class method call output for Public is :  " + superClassA.getPublicState());

        System.out.println("Parent class method call output for Protected is :  " + superClassA.getProtectedState());

        System.out.println("Parent class method call output for Default is :  " + superClassA.getDefaultState());

         //superClassA.getPrivateState() getPrivateState() private method is not visible

    }
}
