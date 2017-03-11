package com.big.data.java.packageA.subPackageA;

import com.big.data.java.packageA.SuperClassA;

public class ClientClassAinSameSubPackage {

    public static void main(String[] args) {

        // accessing superClassA from a client class in the same package as that of the superClass
        SuperClassA superClassA = new SuperClassA();

        //superClassA.getPrivateState() getPrivateState() private method is not visible

        System.out.println("Parent class method call output for Public is :  " + superClassA.getPublicState());

        //System.out.println("Parent class method call output for Protected is :  " + superClassA.getProtectedState());

        //System.out.println("Parent class method call output for Default is :  " + superClassA.getDefaultState());

    }
}
