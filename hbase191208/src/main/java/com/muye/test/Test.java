package com.muye.test;

/**
 * @author : gwh
 * @date : 2019-12-15 20:29
 **/
public class Test {
    public static void main(String[] args) {
//        System.out.println(User.age);
//        System.out.println(Em.age);

        //多态传递
        A a = new C();  // A a = new B();  B b = new C();
        System.out.println(C.class.getInterfaces().length);
    }
}

interface  A{

}

class B implements A{

}

class  C extends B{

}
