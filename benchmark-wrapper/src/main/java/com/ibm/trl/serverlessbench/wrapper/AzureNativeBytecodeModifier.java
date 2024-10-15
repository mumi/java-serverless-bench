package com.ibm.trl.serverlessbench.wrapper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

public class AzureNativeBytecodeModifier {
    public static void modifyJarFile(String pathToJAR) throws Exception {
        Path jarPath = Paths.get(pathToJAR).toAbsolutePath();

        String pathToClassInsideJAR = "io/quarkus/funqy/runtime/bindings/http/VertxRequestHandler.class";
        ClassPool pool = ClassPool.getDefault();
        pool.insertClassPath(jarPath.toString());

        CtClass cc = pool.get("io.quarkus.funqy.runtime.bindings.http.VertxRequestHandler");
        CtMethod method = cc.getDeclaredMethod("dispatch");

        String codeToInsert = "routingContext.response().putHeader(\"X-Azure-Functions-InvocationId\", routingContext.request().headers().get(\"X-Azure-Functions-InvocationId\"));";

        if (!method.getMethodInfo().getCodeAttribute().toString().contains(codeToInsert)) {
            method.insertBefore(codeToInsert);
        }

        Map<String, String> env = new HashMap<>();
        env.put("create", "false");
        URI jarUri = URI.create("jar:" + jarPath.toUri());

        try (FileSystem fs = FileSystems.newFileSystem(jarUri, env)) {
            Path pathInJar = fs.getPath(pathToClassInsideJAR);
            try (OutputStream os = Files.newOutputStream(pathInJar)) {
                cc.toBytecode(new DataOutputStream(os));
            }
            cc.detach();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java BytecodeModifierVertxHandler <path to jar file>");
            System.exit(1);
        }
        String pathToJAR = args[0];
        try {
            modifyJarFile(pathToJAR);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}