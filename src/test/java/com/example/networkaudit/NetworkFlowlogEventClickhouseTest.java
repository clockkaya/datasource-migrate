package com.sama.networkaudit;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.sama.api.networkaudit.bean.NetworkAuditFlowLogV6DO;
import com.sama.api.networkaudit.bean.NetworkFlowlogEventClickhouseDO;
import com.sama.networkaudit.mapper.clickhouse.NetworkFlowlogEventClickhouseMapper;
import com.sama.networkaudit.service.NetworkFlowlogEventClickhouseService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.junit.Test;
//import org.junit.runner.RunWith;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@SpringBootTest(classes = SamaNetworkAuditApplication.class)
public class NetworkFlowlogEventClickhouseTest {

    private static final Logger LOGGER = LogManager.getLogger(NetworkAuditAbtLogTest.class);

    @Resource
    NetworkFlowlogEventClickhouseMapper networkFlowlogEventClickhouseMapper;

    @Resource
    NetworkFlowlogEventClickhouseService networkFlowlogEventClickhouseService;

    /**
     * 测试概览 实时流量
     */
    @Test
    public void  testListFlowTrafficGroupByTimeMapper() throws Exception {
        NetworkFlowlogEventClickhouseDO queryDO = new NetworkFlowlogEventClickhouseDO();
        queryDO.setTenantOrgCode("022500310001000A");
        queryDO.setLogStartTime(this.dateConverter("2025-08-15T13:11:31+08:00[Asia/Shanghai]"));
        queryDO.setLogEndTime(this.dateConverter("2025-08-15T14:11:31+08:00[Asia/Shanghai]"));
        Page<NetworkFlowlogEventClickhouseDO> page =new Page<>();
        List<NetworkFlowlogEventClickhouseDO> networkFlowlogEventClickhouseDOList= networkFlowlogEventClickhouseMapper.listFlowTrafficGroupByTime(queryDO);
        for(NetworkFlowlogEventClickhouseDO networkFlowlogEventClickhouseDO:networkFlowlogEventClickhouseDOList){
            LOGGER.info(networkFlowlogEventClickhouseDO.toString());
        }
    }

    @Test
    public void testSpecifiedPage() throws ParseException {
        NetworkAuditFlowLogV6DO networkAuditFlowLogV6DO=new NetworkAuditFlowLogV6DO();
        networkAuditFlowLogV6DO.setStartTime(this.dateConverter("2025-08-15T13:11:31+08:00[Asia/Shanghai]"));
        networkAuditFlowLogV6DO.setEndTime(this.dateConverter("2025-08-15T14:11:31+08:00[Asia/Shanghai]"));
        List<NetworkAuditFlowLogV6DO> networkAuditFlowLogV6DOS=networkFlowlogEventClickhouseService.specifiedPage("022500310001000A",new NetworkAuditFlowLogV6DO(),0,100,66805L,null);
        for(NetworkAuditFlowLogV6DO networkAuditFlowLogV6DO1:networkAuditFlowLogV6DOS){
            LOGGER.info(networkAuditFlowLogV6DO1.toString());
        }
    }

    Date dateConverter(String dateStr) throws ParseException {
        // 移除时区名称部分，因为 SimpleDateFormat 无法直接解析 [Asia/Shanghai]
        String modifiedDateStr = dateStr.replaceAll("\\[.*\\]", "");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        Date date = sdf.parse(modifiedDateStr);
        return  date;
    }
}
