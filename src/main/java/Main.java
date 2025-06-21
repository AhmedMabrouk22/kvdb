import io.javalin.Javalin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        var app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
                })
                .start(7070);
        printBanner();

        app.before(ctx -> {
            System.out.printf("[REQUEST] %s %s%n",
                    ctx.method(), ctx.fullUrl());
        });

        app.after(ctx -> {
            System.out.printf("[RESPONSE] %s -> Status: %d%n",
                    ctx.fullUrl(), ctx.status().getCode());
        });

        DbEngine db = new DbEngine();

        app.post("/", ctx -> {
            Map<String,String> json = new HashMap<>();
            String key = ctx.queryParam("key");
            String value = ctx.queryParam("value");
            if (key == null || value == null) {
                ctx.status(400).result("Missing key or value\n");
                return;
            }
            db.put(key, value);
            ctx.result("Key: " + key +" inserted\n");
        });

        app.get("/{key}", ctx -> {
            String key = ctx.pathParam("key");
            String value = db.get(key);
            if (value == null || value.isEmpty()) {
                ctx.status(404).result("Key not found\n");
            } else {
                ctx.result("Key: " + key + ", Value: " + value + "\n");
            }
        });

        app.delete("/{key}", ctx -> {
            String key = ctx.pathParam("key");
            db.delete(key);
            ctx.result("Key: " + key + " deleted\n");
        });

    }


    public static void printBanner() {
        String green = "\u001B[32m";
        String blue = "\u001B[34m";
        String reset = "\u001B[0m";

        System.out.println(green + "╔════════════════════════════════════════════╗");
        System.out.println("║           (Key-value) Database             ║");
        System.out.println("╠════════════════════════════════════════════╣");
        System.out.println("║  → Status:       " + blue + "Running" + green + "                   ║");
        System.out.println("║  → Port:         " + blue + "7070" + green + "                      ║");
        System.out.println("║  → Operations:    " + blue + "put, get, delete" + green + "         ║");
        System.out.println("╚════════════════════════════════════════════╝" + reset);
    }

}
