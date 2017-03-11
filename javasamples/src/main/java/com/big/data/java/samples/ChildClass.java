package com.big.data.java.samples;


class ParentClass {

    protected int parentState = 10;

    public String getState(){
        return  "parentState value is  " + parentState;
    }

}

public class ChildClass extends ParentClass{

     private int childSate = 20;

    @Override
    public String getState(){
        return  "childSate value is " + childSate;
    }

    public static void main(String[] args) {

        // parent refrence pointing to the child class
        ParentClass parent = new ChildClass();
        System.out.println("Parent class method call output is :  " + parent.getState());
        System.out.println("state field value of parent is     : " + parent.parentState);


        ChildClass child = (ChildClass) parent;
        System.out.println("Child class method call output is  " + child.getState());

    }

}


