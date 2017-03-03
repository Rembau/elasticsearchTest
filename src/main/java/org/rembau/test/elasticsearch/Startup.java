package org.rembau.test.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by rembau on 2017/3/3.
 */
public class Startup {
    private final static Logger logger = LogManager.getLogger(Startup.class);

    public static void main(String args[]) {
        new ClassPathXmlApplicationContext("classpath:*.xml");

        while(true){
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }
    }
}
