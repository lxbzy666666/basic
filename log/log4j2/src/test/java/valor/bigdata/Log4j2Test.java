package valor.bigdata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class Log4j2Test {
    /**
     * 获取logger对象
     */
    private static final Logger LOGGER = LogManager.getLogger(Log4j2Test.class);

    @Test
    public void log4j2QuickTest() {
        LOGGER.fatal("level:{}", "fatal");
        LOGGER.error("level:{}", "error");
        LOGGER.warn("level:{}", "warn");
        LOGGER.info("level:{}", "info");
        LOGGER.debug("level:{}", "debug");
        LOGGER.trace("level:{}", "trace");
    }
}
