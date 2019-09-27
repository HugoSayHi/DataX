package com.alibaba.datax.plugin.reader.impalareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @className: ImpalaReader
 * @Authors: yuguo
 * @Date: 2019/9/27
 * @Description:
 **/
public class ImpalaReader extends Reader {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */


    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);


        Configuration config = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> list = new ArrayList<>();
            list.add(config);
            return list;
        }

        @Override
        public void init() {
            LOG.debug("impalareader init");
            this.config = super.getPluginJobConf();

            String jdbcUrl = config.getString("jdbcUrl");
            LOG.debug("jdbcUrl:{}", jdbcUrl);

        }

        @Override
        public void destroy() {
            LOG.debug("impalareader destroy");
        }
    }

    public static class Task extends Reader.Task {

        private ImpalaJdbc jdbc = null;
        Configuration config = null;

        @Override
        public void startRead(RecordSender recordSender) {


            Record record = recordSender.createRecord();
            record.addColumn(new StringColumn("tt"));

            recordSender.sendToWriter(record);

        }

        @Override
        public void init() {
            config = this.getPluginJobConf();
            String jdbcUrl = config.getString("jdbcUrl");

            jdbc = new ImpalaJdbc(jdbcUrl);
        }

        @Override
        public void destroy() {

        }
    }

}
