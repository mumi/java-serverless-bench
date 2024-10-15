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

public class AzureBytecodeModifier {
    public static void modifyJarFile(String pathToJAR) throws Exception {
        Path jarPath = Paths.get(pathToJAR).toAbsolutePath();

        String pathToClassInsideJAR = "io/quarkus/azure/functions/resteasy/runtime/Function.class";
        ClassPool pool = ClassPool.getDefault();
        pool.insertClassPath(jarPath.toString());

        CtClass cc = pool.get("io.quarkus.azure.functions.resteasy.runtime.Function");
        CtMethod method = cc.getDeclaredMethod("run");

        String newMethodBody = "{"
                + "com.microsoft.azure.functions.HttpResponseMessage response = dispatch($1);"
                + "com.microsoft.azure.functions.HttpResponseMessage.Builder responseWithInvocationId = $1.createResponseBuilder(response.getStatus()).body(response.getBody()).header(\"X-Azure-Functions-InvocationId\", $2.getInvocationId());"
                + "return responseWithInvocationId.build();"
                + "}";

        if (!method.getMethodInfo().getCodeAttribute().toString().contains("X-Azure-Functions-InvocationId")) {
            method.setBody(newMethodBody);
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
            System.out.println("Usage: java BytecodeModifier <path to jar file>");
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
