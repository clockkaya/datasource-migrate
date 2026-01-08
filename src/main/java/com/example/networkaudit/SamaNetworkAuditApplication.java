package com.sama.networkaudit;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableDubbo
@MapperScan("com.sama.networkaudit.mapper")
@SpringBootApplication(scanBasePackages = {"com.sama", "com.core4ct"})
public class SamaNetworkAuditApplication {

    public static void main(String[] args) {
        SpringApplication.run(SamaNetworkAuditApplication.class, args);
        System.out.println(
                " _____                      ___  _____  _____ \n" +
                        "/  __ \\                    /   |/  __ \\|_   _|\n" +
                        "| /  \\/  ___   _ __  ___  / /| || /  \\/  | |  \n" +
                        "| |     / _ \\ | '__|/ _ \\/ /_| || |      | |  \n" +
                        "| \\__/\\| (_) || |  |  __/\\___  || \\__/\\  | |  \n" +
                        " \\____/ \\___/ |_|   \\___|    |_/ \\____/  \\_/ \n" +
                        "----------------Run Success-------------------"
        );
    }

}
