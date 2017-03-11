package com.big.data.java.packageB;

import com.big.data.java.packageA.SuperClassA;

public class SubClassBInDifferentPackage extends SuperClassA {

    private int subClassBState = 100;

    // Overriden the superClass method
    @Override
    public String getState() {
        return "parentState value is  " + subClassBState;
    }

    public static void main(String[] args) {

        SuperClassA superVar = new SubClassBInDifferentPackage();
        // parent refrence pointing to the child class
        System.out.println("Parent class method call output is :  " + superVar.getState());

        // Child refrence pointing to its own object
        SubClassBInDifferentPackage child = (SubClassBInDifferentPackage) superVar;
        System.out.println("Child class method call output is  " + child.getState());

    }
}
