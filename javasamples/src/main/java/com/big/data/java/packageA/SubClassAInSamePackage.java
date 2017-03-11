package com.big.data.java.packageA;

public class SubClassAInSamePackage extends SuperClassA {

    private int subClassAState = 20;

    //getPrivateState() private method is not visible

    @Override
    public String getPublicState() {
        return "SubClassAInSamePackage value is  " + subClassAState;
    }

    @Override
    protected String getProtectedState() {
        return "SubClassAInSamePackage value is  " + subClassAState;
    }

    @Override
    String getDefaultState() { return "SubClassAInSamePackage value is  " + subClassAState; }

    public static void main(String[] args) {
        // parent refrence pointing to the child class , SuperClassA and SubClassA are in same package
        SuperClassA superVar = new SubClassAInSamePackage();

        System.out.println("Parent class method call output for Public is :  " + superVar.getPublicState());

        System.out.println("Parent class method call output for Protected is :  " + superVar.getProtectedState());

        System.out.println("Parent class method call output for Private is :  " + superVar.getDefaultState());

        // Child refrence pointing to its own object
        SubClassAInSamePackage child = (SubClassAInSamePackage) superVar;
        System.out.println("Child class method call output for Public is  " + child.getPublicState());

        System.out.println("Child class method call output for Protected is  " + child.getProtectedState());

        System.out.println("Child class method call output for Private is  " + child.getDefaultState());

    }
}
