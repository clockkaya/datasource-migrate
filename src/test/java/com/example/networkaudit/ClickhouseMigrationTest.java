package com.sama.networkaudit;

import com.alibaba.fastjson2.JSON;
import com.sama.api.networkaudit.bean.NetworkUserlogEventClickhouseDO;
import com.sama.api.networkaudit.bean.NetworkUserlogEventDO;
import com.sama.networkaudit.mapper.clickhouse.NetworkUserlogEventClickhouseMapper;
import com.sama.networkaudit.service.NetworkFlowlogEventClickhouseService;
import com.sama.networkaudit.service.NetworkSecuritylogEventClickhouseService;
import com.sama.networkaudit.service.NetworkUserlogEventClickhouseService;
import com.sama.networkaudit.service.NetworkUserlogEventService;
import com.sama.networkaudit.utils.data.bean.FlowLogBean;
import com.sama.networkaudit.utils.data.bean.SecurityBean;
import com.sama.networkaudit.utils.data.bean.UserBean;
import jakarta.annotation.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;
import java.util.Vector;

import static com.sama.networkaudit.utils.data.MockDataUtils.generateRandomDateUsingPattern;

/**
 * @author: huxh
 * @description:
 * @datetime: 2025/1/7 10:30
 */
@SpringBootTest(classes = SamaNetworkAuditApplication.class)
public class ClickhouseMigrationTest {

    private final static Logger logger = LogManager.getLogger(ClickhouseMigrationTest.class);

    @Resource
    NetworkUserlogEventClickhouseService networkUserlogEventClickhouseService;

    @Resource
    NetworkUserlogEventService networkUserlogEventService;

    @Resource
    NetworkUserlogEventClickhouseMapper networkUserlogEventClickhouseMapper;

    @Resource
    NetworkFlowlogEventClickhouseService networkFlowlogEventClickhouseService;

    @Resource
    NetworkSecuritylogEventClickhouseService networkSecuritylogEventClickhouseService;

    @Test
    public void userlogMysqlTest() {
        NetworkUserlogEventDO res = networkUserlogEventService.queryById(1839120L);
        logger.info("mysql查询网审用户日志结果:{}", JSON.toJSONString(res));
    }

    private Vector<UserBean> mockUserlogData(Integer size) {
        Vector<UserBean> dataList = new Vector<>();
        // 十天内
        long ago = 1_000 * 60 * 60 * 24 * 10;
        String pattern = "yyyy-MM-dd HH:mm:ss";

        for (int i = 0; i < size; i++) {
            UserBean userBean = new UserBean();
            // copy from mysql
            userBean.setPool_org_code("0224005400011001");
            userBean.setAsset_ip("192.168.11.18");
            userBean.setAsset_id("1658");
            userBean.setAsset_name("网审资产");
            userBean.setInsert_es_time(generateRandomDateUsingPattern(ago, pattern));
            userBean.setVm_ip("192.168.11.177");
            userBean.setVm_id("42203");
            userBean.setKafka_id(UUID.randomUUID().toString().replace("-", ""));
            userBean.setPool_id("87");
            userBean.setTenant_org_code("022500310001000A");
            userBean.setLog_type("web_access");
            userBean.setLog_time(generateRandomDateUsingPattern(ago, pattern));
            userBean.setUser_name("135.224.55.172");
            userBean.setUser_group_name("anonymous");
            userBean.setTerm_platform("未知类型");
            userBean.setTerm_device("未知类型");
            userBean.setSrc_ip("192.168.13.159");
            userBean.setDst_ip("192.168.11.18");
            userBean.setUrl_domain(".224.55.172");
            userBean.setUrl("https://www.srdcloud.cn/smartassist/codefree");
            userBean.setUrl_cate_name("");
            userBean.setHandle_action(0);
            userBean.setMsg("200");
            dataList.add(userBean);
        }
        return dataList;
    }

    /**
     * 通过
     */
    @Test
    public void userlogClickhosueTest() {
        Vector<UserBean> dataList = mockUserlogData(1);
        networkUserlogEventClickhouseService.batchMigrate(dataList);
    }

    @Test
    public void anotherTest(){
        // List<NetworkUserlogEventClickhouseDO> list = networkUserlogEventClickhouseMapper.selectList(null);
        List<NetworkUserlogEventClickhouseDO> list = networkUserlogEventClickhouseMapper.selectAll();
    }

    private Vector<FlowLogBean> mockFlowlogData(Integer size) {
        Vector<FlowLogBean> dataList = new Vector<>();
        // 十天内
        long ago = 1_000 * 60 * 60 * 24 * 10;
        String pattern = "yyyy-MM-dd HH:mm:ss";

        for (int i = 0; i < size; i++) {
            FlowLogBean flowLogBean = new FlowLogBean();
            // copy from es
            flowLogBean.setAsset_name("防篡改win");
            flowLogBean.setPool_org_code("0224005400011001");
            flowLogBean.setVm_ip("192.168.11.33");
            flowLogBean.setVm_id("66806");
            flowLogBean.setKafka_id(UUID.randomUUID().toString().replace("-", ""));
            flowLogBean.setTenant_org_code("022500310001000A");
            flowLogBean.setLog_type("statistic_traffic");
            flowLogBean.setLog_time(generateRandomDateUsingPattern(ago, pattern));
            flowLogBean.setUser_name("10.217.92.101");
            flowLogBean.setUgname("anonymous");
            flowLogBean.setUmac("00:00:00:00:00:00");
            flowLogBean.setUip("192.168.11.210");
            flowLogBean.setAppname("网络协议");
            flowLogBean.setAppgname("apptest2");
            flowLogBean.setUp(1L);
            flowLogBean.setDown(1L);
            flowLogBean.setCreate_time(1703157629L);
            flowLogBean.setEnd_time(1703157629L);
            flowLogBean.setTimes(0L);
            dataList.add(flowLogBean);
        }
        return dataList;
    }

    /**
     * 通过
     */
    @Test
    public void flowlogClickhosueTest() {
        Vector<FlowLogBean> dataList = mockFlowlogData(1);
        networkFlowlogEventClickhouseService.batchMigrate(dataList);
    }

    private Vector<SecurityBean> mockSecuritylogData(Integer size) {
        Vector<SecurityBean> dataList = new Vector<>();
        // 十天内
        long ago = 1_000 * 60 * 60 * 24 * 10;
        String pattern = "yyyy-MM-dd HH:mm:ss";

        for (int i = 0; i < size; i++) {
            SecurityBean securityBean = new SecurityBean();
            // copy from es
            securityBean.setPriority(0);
            securityBean.setNotice_type(0);
            securityBean.setNotice_object(0);
            securityBean.setProduct_sub_table_num(0);
            securityBean.setPool_org_code("0224005400011001");
            securityBean.setAsset_ip("192.168.11.176");
            securityBean.setVirus_name("virus01");
            securityBean.setFile_name("virus.zip");
            securityBean.setLog_type("av");
            securityBean.setUser_id("0");
            securityBean.setUser_name("192.168.10.75");
            securityBean.setPolicy_id("1");
            securityBean.setSrc_mac("fa:16:3e:33:fb:2e");
            securityBean.setDst_mac("fa:16:3e:06:e4:e0");
            securityBean.setSrc_ip("192.168.13.159");
            securityBean.setDst_ip("192.168.11.176");
            securityBean.setSrc_port("57152");
            securityBean.setDst_port("12345");
            securityBean.setProtocol("TCP");
            securityBean.setLevel("warning");
            securityBean.setCtime(generateRandomDateUsingPattern(ago, pattern));
            securityBean.setAction("block");
            securityBean.setKafka_id(UUID.randomUUID().toString().replace("-", ""));
            securityBean.setVm_ip("192.168.11.163");
            securityBean.setAsset_id("27");
            securityBean.setAsset_name("主机漏扫");
            securityBean.setVm_id("32010");
            securityBean.setTenant_org_code("022700110001");
            securityBean.setApp_name("HTTP文件下载");
            securityBean.setApp_protocol("HTTP");

            securityBean.setInsert_es_time("多余的字段");

            dataList.add(securityBean);
        }
        return dataList;
    }

    /**
     * 通过
     */
    @Test
    public void securitylogClickhosueTest() {
        Vector<SecurityBean> dataList = mockSecuritylogData(1);
        networkSecuritylogEventClickhouseService.batchMigrate(dataList);
    }
}
