package com.big.data.java.samples;

class SuperClass {
    final int b;

    public SuperClass(int c) {
        this.b = c;
        System.out.println("I am inside super class Constructor  :   " + b);
    }
}

public class SubClass extends SuperClass {

    private String state = initializerMethod();

    public SubClass() {
        super(1);
        System.out.println("I am inside Constructor");
        System.out.println("value of state inside Constructor, before is  : " + state);
        state = "constructor";
        System.out.println("value of state inside Constructor, after  is  : " + state);
    }

    private String initializerMethod() {
        System.out.println("I am inside instance Method initialize ");
        return "method Initializer";
    }

    {
        System.out.println("I am inside instance initialize Block");
        System.out.println("value of state inside initializer Block is " + state);
        state = "initializer Block";
    }

    public static void main(String[] args) {
        SubClass reference = new SubClass();
    }

}