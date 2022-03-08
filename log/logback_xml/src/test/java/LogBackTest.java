import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试slf4j + logback 的日志系统的实现
 */
public class LogBackTest {

    /**
     * 获取日志器对象
     */
    private static final Logger logger = LoggerFactory.getLogger(LogBackTest.class);

    /**
     * 使用logback的默认配置进行日志记录：
     * 默认level是debug, appender is consoleAppender,
     */
    @Test
    public void logbackQuickTest() {
        logger.error("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "error", "gary", 23, System.currentTimeMillis());
        logger.warn("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "warn", "gary", 23, System.currentTimeMillis());
        logger.info("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "info", "gary", 23, System.currentTimeMillis());
        logger.debug("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "debug", "gary", 23, System.currentTimeMillis());
        logger.trace("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "trace", "gary", 23, System.currentTimeMillis());
    }

    /**
     * 使用logback的自定义配置 ： resource/logback-test.xml
     */
    @Test
    public void logbackXmlTest() {
        for (int i = 0; i < 10000; i++) {
            logger.error("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "error", "gary", 23, System.currentTimeMillis());
            logger.warn("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "warn", "gary", 23, System.currentTimeMillis());
            logger.info("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "info", "gary", 23, System.currentTimeMillis());
            logger.debug("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "debug", "gary", 23, System.currentTimeMillis());
            logger.trace("[level] is {} :this is slf4j + logback, and the author is {},name  is {}, date is {}", "trace", "gary", 23, System.currentTimeMillis());
        }

    }
}
