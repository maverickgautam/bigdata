package com.big.data.java.packageA.subPackageA;

import com.big.data.java.packageA.SuperClassA;

public class SubClassAinSameSubPackage extends SuperClassA {

    private int subClassAState = 30;

    // Overriden the superClass method
    @Override
    public String getState() {
        return "parentState value is  " + subClassAState;
    }

    public static void main(String[] args) {
        // parent refrence pointing to the child class , SubClassSubPackageA extends SuperClassA and is in a subpackage
        SuperClassA superVar = new SubClassAinSameSubPackage();
        System.out.println("Parent class method call output is :  " + superVar.getState());

        // Child refrence pointing to its own object
        SubClassAinSameSubPackage child = (SubClassAinSameSubPackage) superVar;
        System.out.println("Child class method call output is  " + child.getState());

    }
}
