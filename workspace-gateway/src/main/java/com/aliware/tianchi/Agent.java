package com.aliware.tianchi;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;

/**
 * @author weicheng.zhao
 * @date 2019/7/3
 */
public class Agent {

    private static final Logger LOGGER = LoggerFactory.getLogger(Agent.class);
    private static final String CLASS_NAME="com.aliware.tianchi.Test";

    static {
        System.out.println("动态注入初始化");
    }

    public static void agentmain (String agentArgs, Instrumentation instrumentation){
        System.out.println("动态注入开始");
        ClassFileTransformer transformer = new ResponseCallbackClassFileTransformer();
        instrumentation.addTransformer(transformer);
    }

    private static class ResponseCallbackClassFileTransformer implements ClassFileTransformer {

        @Override
        public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
            System.out.println("动态注入 "+className);
            String classNamePoint=className.replaceAll("/", ".");
            if(CLASS_NAME.equals(className)) {
                ClassPool classPool = ClassPool.getDefault();

                try {
                    CtClass ctClass = classPool.get(classNamePoint);
                    CtMethod ctMethod = ctClass.getDeclaredMethod("test");
                    if(!ctMethod.isEmpty()) {
                        ctMethod.insertBefore("System.out.println(\"before hello!!!\");");
                    }
                    return ctClass.toBytecode();
                } catch (Exception e) {
                    LOGGER.info("动态注入异常 {}",className,e);
                }
            }
            return null;
        }
    }
}
