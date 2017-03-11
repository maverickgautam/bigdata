package com.big.data.java.packageB.subPackageB;

import com.big.data.java.packageA.SuperClassA;

public class SubClassBInDifferentSubPackage extends SuperClassA {

    private int subClassBState = 100;

    // Overriden the superClass method

    //getPrivateState() private method is not visible

    @Override
    public String getPublicState() {
        return "SubClassBInDifferentSubPackage value is  " + subClassBState;
    }

    @Override
    protected String getProtectedState() {
        return "SubClassBInDifferentSubPackage value is  " + subClassBState;
    }

    //Default is not visibleString getDefaultState() { return "SubClassBInDifferentPackage value is  " + subClassBState; }


    public static void main(String[] args) {

        SuperClassA superVar = new SubClassBInDifferentSubPackage();
        // parent refrence pointing to the child class
        // parent refrence pointing to the child class
        System.out.println("Parent class method call output is :  " + superVar.getPublicState());

        System.out.println("Parent class method call output for Public is :  " + superVar.getPublicState());

        //System.out.println("Parent class method call output for Protected is :  " + superVar.getProtectedState());

        //System.out.println("Parent class method call output for Private is :  " + superVar.getDefaultState());

        // Child refrence pointing to its own object
        SubClassBInDifferentSubPackage child = (SubClassBInDifferentSubPackage) superVar;

        System.out.println("Child class method call output for Public is  " + child.getPublicState());

        System.out.println("Child class method call output for Protected is  " + child.getProtectedState());

        //System.out.println("Child class method call output for Private is  " + child.getDefaultState());

    }
}
