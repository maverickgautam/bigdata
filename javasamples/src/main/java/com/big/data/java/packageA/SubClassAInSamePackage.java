package com.big.data.java.packageA;

public class SubClassAInSamePackage extends SuperClassA {

    private int subClassAState = 20;

    // Overriden the superClass method
    @Override
    public String getState() {
        return "parentState value is  " + subClassAState;
    }

    public static void main(String[] args) {
        // parent refrence pointing to the child class , SuperClassA and SubClassA are in same package
        SuperClassA superVar = new SubClassAInSamePackage();
        System.out.println("Parent class method call output is :  " + superVar.getState());

        // Child refrence pointing to its own object
        SubClassAInSamePackage child = (SubClassAInSamePackage) superVar;
        System.out.println("Child class method call output is  " + child.getState());

    }
}
