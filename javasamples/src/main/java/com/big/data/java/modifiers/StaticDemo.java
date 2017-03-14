package com.big.data.java.modifiers;

public class StaticDemo {

    // Default value if int is 0 , objects is null both for static and instance fields
    private static int staticState;

    static {
        // Executed when class is loaded by JVM
        System.out.println("Inside Static Initializer block staticState value before is " + staticState);
        staticState = 10;
        System.out.println("Inside Static Initializer block staticState value before is " + staticState);
        // state = 20  not possible as state is part of object not class
    }

    private int state;

    public StaticDemo() {
        state = 10;
        System.out.println("Inside Constructor staticState value before is " + staticState);
        // Static field is accessible from non static methods and constructors
        staticState = 30;
    }

    // all methods are part of class and not of object .
    // the significance of static methods is it is not accessing any instance members directly
    // Static method refer class behaviour and not object behaviour
    private static int getStaticMethod() {
        return staticState;
        //state = 30;  non static field is not accessible withing static methods
    }

    public static void main(String[] args) {
        StaticDemo reference = new StaticDemo();
        // Static method is accessible via class Name .
        StaticDemo.getStaticMethod();
    }

    // Static innerClass signifies it will not hold the reference of the outer class.
    // A non static innerClass holds the reference of the outer class
    private static class InnerClass {
        private int a;
    }

}
