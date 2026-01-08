package com.sama.networkaudit;

import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.support.Pagination;
import com.core4ct.utils.DateUtils;
import com.core4ct.utils.PageUtils;
import com.sama.api.networkaudit.bean.*;
import com.sama.api.networkaudit.service.NetworkAuditReportDubboService;
import com.sama.networkaudit.mapper.clickhouse.*;
import com.sama.networkaudit.service.*;
import com.sama.networkaudit.utils.CommonUtils;
import jakarta.annotation.Resource;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author: huxh
 * @description:
 * @datetime: 2025/3/31 16:03
 */
@SpringBootTest(classes = SamaNetworkAuditApplication.class)
public class ClickhouseAfterMigrationTest {

    private final static Logger logger = LogManager.getLogger(ClickhouseAfterMigrationTest.class);

    @Resource
    NetworkUserlogEventClickhouseService networkUserlogEventClickhouseService;

    @Resource
    NetworkUserlogEventClickhouseMapper networkUserlogEventClickhouseMapper;

    @Resource
    NetworkFlowlogEventClickhouseService networkFlowlogEventClickhouseService;

    @Resource
    NetworkSecuritylogEventClickhouseService networkSecuritylogEventClickhouseService;

    @DubboReference
    NetworkAuditReportDubboService networkAuditReportDubboService;

    @Resource
    WebBehaviorStatsClickhouseMapper webBehaviorStatsClickhouseMapper;

    @Resource
    WebBehaviorStatsClickhouseService webBehaviorStatsClickhouseService;

    @Resource
    WebSiteReqStatsClickhouseMapper webSiteReqStatsClickhouseMapper;

    @Resource
    WebSiteReqStatsClickhouseService webSiteReqStatsClickhouseService;

    @Resource
    IpsTypeStatsClickhouseMapper ipsTypeStatsClickhouseMapper;

    @Resource
    IpsTypeStatsClickhouseService ipsTypeStatsClickhouseService;

    @Resource
    MaliciousStatsClickhouseMapper maliciousStatsClickhouseMapper;

    @Resource
    MaliciousStatsClickhouseService maliciousStatsClickhouseService;

    @Resource
    VirusStatsClickhouseMapper virusStatsClickhouseMapper;

    @Resource
    VirusStatsClickhouseService virusStatsClickhouseService;

    @Resource
    AppFlowStatsClickhouseMapper appFlowStatsClickhouseMapper;

    @Resource
    AppFlowStatsClickhouseService appFlowStatsClickhouseService;

    @Resource
    IpFlowStatsClickhouseMapper ipFlowStatsClickhouseMapper;

    @Resource
    IpFlowStatsClickhouseService ipFlowStatsClickhouseService;

    @Test
    public void userPageByCondAndOrdTest(){
        NetworkUserlogEventClickhouseDO queryDO = new NetworkUserlogEventClickhouseDO();
        queryDO.setTenantOrgCode("0224005400011003");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-01-01 00:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-03-31 00:00:00"));
        queryDO.setLogTypes(CommonUtils.convertStringToList("web_access,search_engine,mail"));
        queryDO.setSrcIp("192");
        queryDO.setDstIp("128");

        Pagination<NetworkUserlogEventClickhouseDO> rowBounds = new Pagination<>(1, 10);
        Page<NetworkUserlogEventClickhouseDO> page = PageUtils.getPage(rowBounds);

        List<NetworkUserlogEventClickhouseDO> res = networkUserlogEventClickhouseMapper.pageByCondAndOrd(queryDO, page);
        logger.info("【测试】 selectListByCondAndOrd 结果: {}", JSON.toJSONString(res));
    }

    /**
     * https://tenant.sama.dev.ctnisi.cn:30443/tenant/v1/service/networkAudit/log/pageV6
     *
     * request:
     * {
     *     "current": 1,
     *     "src_ip": "192",
     *     "daterange": [
     *         "2025-03-16 00:00:00",
     *         "2025-03-31 23:59:59"
     *     ],
     *     "logEndTime": "2025-03-31 23:59:59",
     *     "logStartTime": "2025-03-16 00:00:00",
     *     "orderBy": "",
     *     "dst_ip": "192",
     *     "size": 10,
     *     "log_type": "web_access,search_engine",
     *     "log_typeArr": [
     *         "web_access",
     *         "search_engine"
     *     ],
     *     "logClass": 0
     * }
     */
    @Test
    public void userSpecifiedPageTest(){
        String tenantOrgCode = "022500310001000A";
        NetworkAuditUserLogDO inputParams = new NetworkAuditUserLogDO();
        inputParams.setStartTime(DateUtils.stringToDate("2025-04-14 10:07:23"));
        inputParams.setEndTime(DateUtils.stringToDate("2025-04-21 10:07:23"));
        inputParams.setLog_type("web_access,search_engine,mail");
        inputParams.setSrc_ip("192.168.10.75");
        inputParams.setDst_ip("192.168.10.75");
        networkUserlogEventClickhouseService.specifiedPage(tenantOrgCode, inputParams, 1, 10);
    }

    /**
     * https://tenant.sama.dev.ctnisi.cn:30443/tenant/v1/service/networkAudit/log/pageV6
     *
     * {
     *     "current": 1,
     *     "dbname": "",
     *     "daterange": [
     *         "2024-10-01 00:00:00",
     *         "2024-10-31 23:59:59"
     *     ],
     *     "logEndTime": "2024-10-31 23:59:59",
     *     "logStartTime": "2024-10-01 00:00:00",
     *     "orderBy": "log_time desc",
     *     "severity": "",
     *     "size": 10,
     *     "srcIpv4": "",
     *     "srcPort": "",
     *     "src_ip": "192",
     *     "dst_ip": "192",
     *     "log_type": "ips,av,malware_app",
     *     "log_typeArr": [
     *         "ips",
     *         "av",
     *         "malware_app"
     *     ],
     *     "logClass": 1
     * }
     */
    @Test
    public void securitySpecifiedPageTest(){
        String tenantOrgCode = "0225003";
        NetworkAuditSecurityLogDO inputParams = new NetworkAuditSecurityLogDO();
        inputParams.setStartTime(DateUtils.stringToDate("2025-04-08 00:00:00"));
        inputParams.setEndTime(DateUtils.stringToDate("2025-04-16 00:00:00"));
        inputParams.setLog_type("ips,av,malware_app");
        inputParams.setSrc_ip("192.168");
        inputParams.setDst_ip("192.168");
        networkSecuritylogEventClickhouseService.specifiedPage(tenantOrgCode, inputParams, 1, 10);
    }

    @Test
    public void flowOverviewQueryTest(){
        String tenantOrgCode = "01250036";
        Date logStartTime = DateUtils.stringToDate("2025-03-01 00:00:00");
        Date logEndTime = DateUtils.stringToDate("2025-03-31 00:00:00");
        Long vmId = 341L;
        // 忽略Internal情况
        String poolId = "";
        networkFlowlogEventClickhouseService.overviewQuery(tenantOrgCode, logStartTime, logEndTime, vmId, poolId);
    }

    @Test
    public void securityOverviewQueryTest(){
        String tenantOrgCode = "01250036";
        Date logStartTime = DateUtils.stringToDate("2025-03-01 00:00:00");
        Date logEndTime = DateUtils.stringToDate("2025-03-31 00:00:00");
        Long vmId = 341L;
        // 忽略Internal情况
        String poolId = "";
        networkSecuritylogEventClickhouseService.overviewQuery(tenantOrgCode, logStartTime, logEndTime, vmId, poolId);
    }

    @Test
    public void flowSpecifiedPageTest(){
        String tenantOrgCode = "01250036";
        NetworkAuditFlowLogV6DO inputParams = new NetworkAuditFlowLogV6DO();
        inputParams.setStartTime(DateUtils.stringToDate("2025-03-01 00:00:00"));
        inputParams.setEndTime(DateUtils.stringToDate("2025-03-31 00:00:00"));
        inputParams.setLogType("statistic_traffic, other");
        Long vmId = 341L;
        // 忽略Internal情况
        String poolId = "";
        networkFlowlogEventClickhouseService.specifiedPage(tenantOrgCode, inputParams, 1, 10, vmId, poolId);
    }

    // ==================================================
    // 定时任务 7
    // ==================================================

    @Test
    public void webBehaviorStatsTest(){
        // List<WebBehaviorStatsClickhouseDO> plainRes = webBehaviorStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 webBehaviorStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        WebBehaviorStatsClickhouseDO queryDO = new WebBehaviorStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("tenant_org_code", "assetId", "event_time"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-03 14:00:00"));
        queryDO.setOrderBy("asset_id DESC");
        queryDO.setLimit(1);
        List<WebBehaviorStatsClickhouseDO> customRes = webBehaviorStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 webBehaviorStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void webBehaviorStatsServiceTest(){
        // webBehaviorStatsClickhouseService.getWebBehaviorInfo("02250031", "2025-04-03 10:00:00", "2025-04-03 14:00:00");
        Map<String, Object> ckRes = webBehaviorStatsClickhouseService.getWebBehaviorSummary("02250031", "2025-04-01 00:00:00", "2025-04-22 00:00:00");
        NetAuditSummaryInfo netAuditSummaryInfo = new NetAuditSummaryInfo();
        netAuditSummaryInfo.setWebReqCnt((Long)ckRes.getOrDefault("webCnt", 0));
        netAuditSummaryInfo.setWebSearchCnt((Long)ckRes.getOrDefault("searchCnt", 0));
        netAuditSummaryInfo.setMailCnt((Long)ckRes.getOrDefault("mailCnt", 0));
        logger.info("【整合|NetAudit】 最终格式 Map<String, Object>: {}", JSON.toJSONString(netAuditSummaryInfo));
    }

    @Test
    public void webSiteReqStatsTest(){
        // List<WebSiteReqStatsClickhouseDO> plainRes = webSiteReqStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 webSiteReqStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        WebSiteReqStatsClickhouseDO queryDO = new WebSiteReqStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("event_time", "url"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-03 11:00:00"));
        queryDO.setOrderBy("url DESC");
        queryDO.setLimit(10);
        List<WebSiteReqStatsClickhouseDO> customRes = webSiteReqStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 webSiteReqStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void webSiteReqStatsServiceTest(){
        // webSiteReqStatsClickhouseService.getWebSiteReqInfo("02250031", "2025-04-01 10:00:00", "2025-04-01 11:00:00");
        webSiteReqStatsClickhouseService.getWebSiteReqSum("02250031", "2025-03-03 10:00:00", "2025-03-07 11:00:00");
    }

    @Test
    public void ipsTypeStatsTest(){
        // List<IpsTypeStatsClickhouseDO> plainRes = ipsTypeStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 ipsTypeStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        IpsTypeStatsClickhouseDO queryDO = new IpsTypeStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("tenant_org_code", "event_time", "event_type"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-03 11:00:00"));
        queryDO.setOrderBy("count DESC");
        queryDO.setLimit(10);
        List<IpsTypeStatsClickhouseDO> customRes = ipsTypeStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 ipsTypeStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void ipsTypeStatsServiceTest(){
        // ipsTypeStatsClickhouseService.getIpsTypeStats("02250031", "2025-04-03 10:00:00", "2025-04-03 11:00:00");
        // ipsTypeStatsClickhouseService.getIpsTypeTrendByHour("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
        // ipsTypeStatsClickhouseService.getIpsTypeTrendByDay("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
        ipsTypeStatsClickhouseService.getIpsSum("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
    }

    @Test
    public void maliciousStatsTest(){
        // List<MaliciousStatsClickhouseDO> plainRes = maliciousStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 maliciousStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        MaliciousStatsClickhouseDO queryDO = new MaliciousStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("tenant_org_code", "web_name"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-03 11:00:00"));
        queryDO.setOrderBy("count DESC");
        queryDO.setLimit(10);
        List<MaliciousStatsClickhouseDO> customRes = maliciousStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 maliciousStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void maliciousStatsServiceTest(){
        // maliciousStatsClickhouseService.getMaliciousStats("02250031", "2025-04-03 10:00:00", "2025-04-03 11:00:00");
        maliciousStatsClickhouseService.getMaliciousSum("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
    }

    @Test
    public void virusStatsTest(){
        // List<VirusStatsClickhouseDO> plainRes = virusStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 virusStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        VirusStatsClickhouseDO queryDO = new VirusStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("tenant_org_code", "virus_name"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-08 11:00:00"));
        queryDO.setOrderBy("count DESC");
        queryDO.setLimit(10);
        List<VirusStatsClickhouseDO> customRes = virusStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 virusStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void virusStatsServiceTest(){
        // virusStatsClickhouseService.getVirusStats("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
        virusStatsClickhouseService.getVirusSum("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
    }

    @Test
    public void appFlowStatsTest(){
        // List<AppFlowStatsClickhouseDO> plainRes = appFlowStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 appFlowStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        AppFlowStatsClickhouseDO queryDO = new AppFlowStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("tenant_org_code", "virus_name"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-08 11:00:00"));
        queryDO.setOrderBy("total_bytes DESC");
        queryDO.setLimit(10);
        List<AppFlowStatsClickhouseDO> customRes = appFlowStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 appFlowStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void appFlowStatsServiceTest(){
        appFlowStatsClickhouseService.getAppFlowOverview("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
    }

    @Test
    public void ipFlowStatsTest(){
        // List<IpFlowStatsClickhouseDO> plainRes = ipFlowStatsClickhouseMapper.plainSelect();
        // logger.info("【测试】 ipFlowStatsClickhouseMapper.plainSelect 结果:\n{}", JSON.toJSONString(plainRes));

        IpFlowStatsClickhouseDO queryDO = new IpFlowStatsClickhouseDO();
        queryDO.setAggregatedColumns(Arrays.asList("asset_id"));
        queryDO.setTenantOrgCode("02250031");
        queryDO.setLogStartTime(DateUtils.stringToDate("2025-04-03 10:00:00"));
        queryDO.setLogEndTime(DateUtils.stringToDate("2025-04-08 11:00:00"));
        queryDO.setOrderBy("total_bytes DESC");
        queryDO.setLimit(10);
        List<IpFlowStatsClickhouseDO> customRes = ipFlowStatsClickhouseMapper.customSelect(queryDO);
        logger.info("【测试】 ipFlowStatsClickhouseMapper.customSelect 结果:\n{}", JSON.toJSONString(customRes));
    }

    /**
     * pass
     */
    @Test
    public void ipFlowStatsServiceTest(){
        ipFlowStatsClickhouseService.getFlowOverview("02250031", "2025-04-03 10:00:00", "2025-04-08 11:00:00");
    }

    /**
     * (orgCodePrefix, startTime, endTime);
     * (orgCodePrefix, esStartTime, esEndTime);
     *
     * @throws InterruptedException
     */
    @Test
    public void dubboTests() throws InterruptedException {
        String orgCodePrefix = "02250031";
        String startTime = "2025-04-09 00:00:00";
        String endTime = "2025-04-10 00:00:00";
        String esStartTime = "2024-01-01 00:00:00";
        String esEndTime = "2024-12-31 00:00:00";

        // networkAuditReportDubboService.getNetAuditSummaryInfo(orgCodePrefix, esStartTime, esEndTime);
        networkAuditReportDubboService.getWebBehaviorInfo(orgCodePrefix, startTime, endTime);
        // networkAuditReportDubboService.getWebSiteReqInfo(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getWebReqSumFromWebSiteReqStats(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getIpsTypeStats(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getIpsTypeTrend(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getMaliciousStats(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getVirusStats(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getAppFlowOverview(orgCodePrefix, esStartTime, esEndTime);
        // networkAuditReportDubboService.getFlowOverview(orgCodePrefix, esStartTime, esEndTime);

        Thread.sleep(5_000);
    }

}
