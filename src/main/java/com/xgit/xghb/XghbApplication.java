package com.xgit.xghb;

import com.xgit.xghb.excutor.TestTwo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class XghbApplication {
    public static final String[] TOPIC_NAMES = new String[]{"testEnvXinXin","testEnvGroup","testEnvExternal"};
    public static void main(String[] args) {
        SpringApplication.run(XghbApplication.class, args);
        TestTwo.main(args);
    }

}
