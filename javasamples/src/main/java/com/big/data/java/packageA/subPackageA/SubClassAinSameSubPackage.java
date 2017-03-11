package com.big.data.java.packageA.subPackageA;

import com.big.data.java.packageA.SuperClassA;

public class SubClassAinSameSubPackage extends SuperClassA {

    private int subClassAState = 30;

    // Overriden the superClass method

    //getPrivateState() private method is not visible

    @Override
    public String getPublicState() {
        return "SubClassAinSameSubPackage value is  " + subClassAState;
    }

    @Override
    protected String getProtectedState() {
        return "SubClassAinSameSubPackage value is  " + subClassAState;
    }

    // Default is not accessible String getDefaultState(){ return "SubClassAinSameSubPackage value is  " + subClassAState; }

    //getPrivateState() private method is not visible

    public static void main(String[] args) {
        // parent refrence pointing to the child class , SubClassSubPackageA extends SuperClassA and is in a subpackage
        SuperClassA superVar = new SubClassAinSameSubPackage();
        System.out.println("Parent class method call output for Public is :  " + superVar.getPublicState());

        //Protected Field not visible System.out.println("Parent class method call output is :  " + superVar.getProtectedState());

        //Default Field not visible System.out.println("Parent class method call output is :  " + superVar.getDefaultState());

        // Child refrence pointing to its own object
        SubClassAinSameSubPackage child = (SubClassAinSameSubPackage) superVar;
        System.out.println("Child class method call output for Public for Public is  " + child.getPublicState());

        System.out.println("Child class method call output for Protected is  " + child.getProtectedState());

        //Default Field not visible  System.out.println("Child class method call output is  " + child.getDefaultState());

    }
}
