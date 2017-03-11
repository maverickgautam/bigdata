package com.big.data.java.packageA;

public class SuperClassA {

    private int superClassAState = 10;

    public String getPublicState() {
        return "parentState value is  " + superClassAState;
    }

    private String getPrivateState() {
        return new String("I am not available for inheritance");
    }

    protected String getProtectedState(){ return "parentState value is  " + superClassAState; }

    String getDefaultState(){ return "parentState value is  " + superClassAState; }


}
