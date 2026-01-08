package com.sama.networkaudit.dubboImpl;

import com.alibaba.fastjson2.JSON;
import com.core4ct.utils.DataUtils;
import com.core4ct.utils.DateUtils;
import com.sama.api.baseability.bean.VMwareDevice;
import com.sama.api.baseability.service.PowerDubboService;
import com.sama.api.business.TOrderDubboService;
import com.sama.api.business.object.AssetDO;
import com.sama.api.business.object.DO.order.TOrderServiceAbilityDO;
import com.sama.api.data.object.DTO.search.DataBean;
import com.sama.api.data.object.DTO.search.NetWorkDataSearchDTO;
import com.sama.api.data.object.DTO.search.NetWorkWebSearchDTO;
import com.sama.api.data.object.DTO.search.NetworkSecurityStatisticsDTO;
import com.sama.api.data.object.DTO.search.network.SecurityBean;
import com.sama.api.data.object.DTO.search.network.flowBean.FlowBean;
import com.sama.api.data.object.DTO.search.network.securityPagingBean;
import com.sama.api.data.object.DTO.search.network.userBean.UserBean;
import com.sama.api.data.object.DTO.search.network.web.NetWorkWebDataBean;
import com.sama.api.data.object.DTO.search.network.web.WebAppBean;
import com.sama.api.data.object.DTO.search.network.web.WebIpBean;
import com.sama.api.data.service.NetWorkSearchDubboService;
import com.sama.api.networkaudit.bean.*;
import com.sama.api.networkaudit.service.NetworkAuditLogDubboService;
import com.sama.api.pool.object.DO.VMDO;
import com.sama.api.pool.service.AssetVmInfoDubboService;
import com.sama.networkaudit.service.*;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import jakarta.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.sama.networkaudit.constant.Constants.UNOFFICIAL_ENVS;

@DubboService
@RefreshScope
public class NetworkAuditLogDubboServiceImpl implements NetworkAuditLogDubboService {

    private final static Logger logger = LogManager.getLogger();

    @DubboReference
    NetWorkSearchDubboService netWorkSearchDubboService;

    @DubboReference
    PowerDubboService powerService;

    @DubboReference
    TOrderDubboService tOrderDubboService;

    @DubboReference
    AssetVmInfoDubboService assetVmInfoDubboService;

    @Resource
    FlowStatService flowStatService;

    @Resource
    NetworkUserlogEventService networkUserlogEventService;

    @Resource
    NetworkSecurityEventService networkSecurityEventService;

    @Value("${init.env}")
    private String env;

    @Resource(name = "validDeprecatedFunc")
    ThreadPoolTaskExecutor validDeprecatedFunc;

    @Resource
    NetworkFlowlogEventClickhouseService networkFlowlogEventClickhouseService;

    @Resource
    NetworkUserlogEventClickhouseService networkUserlogEventClickhouseService;

    @Resource
    NetworkSecuritylogEventClickhouseService networkSecuritylogEventClickhouseService;

    @Override
    public List<NetworkAuditFlowLogV6DO> flowLogPage(String tenantOrgCode, NetworkAuditFlowLogV6DO networkAuditFlowLogV6DO, Integer current, Integer size, Boolean isInternal, Long vmId) throws Exception {
        String poolId = "";
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = DataUtils.isNotEmpty(vMwareDevice.getPoolId()) ? String.valueOf(vMwareDevice.getPoolId()) : poolId;
        }
        List<NetworkAuditFlowLogV6DO> ckRes = networkFlowlogEventClickhouseService.specifiedPage(tenantOrgCode, networkAuditFlowLogV6DO, current, size, vmId, poolId);

        // 使用新线程调用 flowLogPageES 方法，并校验（日志在方法内打印）
        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【page|流量】 开始调用 flowLogPageES 旧方法验证 ---------->");
                try {
                    flowLogPageES(tenantOrgCode, networkAuditFlowLogV6DO, current, size, isInternal, vmId);
                } catch (Exception e) {
                    logger.error("捕获小异常一只，堆栈信息如下: ", e);
                }
            }
        });

        return ckRes;
    }

    @Deprecated
    private List<NetworkAuditFlowLogV6DO> flowLogPageES(String tenantOrgCode, NetworkAuditFlowLogV6DO networkAuditFlowLogV6DO, Integer current, Integer size, Boolean isInternal, Long vmId) throws Exception {

        Long poolId = null;
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = vMwareDevice.getPoolId();
        }

        NetWorkDataSearchDTO netWorkDataSearchDTO = new NetWorkDataSearchDTO();
        List<NetworkAuditFlowLogV6DO> resultList = new LinkedList<>();
        netWorkDataSearchDTO.setPageNum(current);
        netWorkDataSearchDTO.setPageSize(size);
        if (vmId != null){
            netWorkDataSearchDTO.setVmId("" + vmId);
        }
        if (DataUtils.isNotEmpty(tenantOrgCode)){
            netWorkDataSearchDTO.setTenant_org_code(tenantOrgCode);
        }
        if (networkAuditFlowLogV6DO.getStartTime() != null){
            String startTime = DateUtils.format(networkAuditFlowLogV6DO.getStartTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            netWorkDataSearchDTO.setStartTime(startTime);
        }
        if (networkAuditFlowLogV6DO.getEndTime() != null){
            String endTime = DateUtils.format(networkAuditFlowLogV6DO.getEndTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            netWorkDataSearchDTO.setEndTime(endTime);
        }
        if (DataUtils.isNotEmpty(networkAuditFlowLogV6DO.getLogType())){
            netWorkDataSearchDTO.setLog_type(networkAuditFlowLogV6DO.getLogType());
        }
        if (DataUtils.isNotEmpty(networkAuditFlowLogV6DO.getOrderBy())){
            netWorkDataSearchDTO.setSort(networkAuditFlowLogV6DO.getOrderBy().replace(" ", ":"));
        }
        if(!isInternal){
            netWorkDataSearchDTO.setPool_id(String.valueOf(poolId));
        }


        logger.info("查询网审实时流量日志请求参数信息:{}", JSON.toJSONString(netWorkDataSearchDTO));
        DataBean<FlowBean> response = netWorkSearchDubboService.pagingQuery(netWorkDataSearchDTO);
        logger.info("查询网审实时流量日志响应信息:{}", response);
        Assert.notNull(response, "[接口调用异常,请检查调用参数]查询网审实时流量日志响应信息为空");
        List<FlowBean> data = response.getDatas();
        if (DataUtils.isEmpty(data)){
            logger.info("网审实时流量日志为空");
        }
        for (FlowBean log : data) {
            String json = JSON.toJSONString(log);
            NetworkAuditFlowLogV6DO logDO = JSON.parseObject(json, NetworkAuditFlowLogV6DO.class);
            logDO.setLogType(log.getLog_type());
            logDO.setLogTime(log.getLog_time());
            logDO.setUserName(log.getUser_name());
            resultList.add(logDO);
        }

        logger.info("【page|流量】 旧方法验证结束 ----------> {}", JSON.toJSONString(resultList));
        return resultList;
    }

    @Override
    public OverviewInfo flowLogAndAppLogOverviewData(String tenantOrgCode, Date logStartTime, Date logEndTime, Boolean isInternal, Long vmId) throws Exception {
        String poolId = "";
        if(!isInternal){
            // OPT： 这里应该可以二选一优化
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = DataUtils.isNotEmpty(vMwareDevice.getPoolId()) ? String.valueOf(vMwareDevice.getPoolId()) : poolId;
        }
        OverviewInfo ckRes = networkFlowlogEventClickhouseService.overviewQuery(tenantOrgCode, logStartTime, logEndTime, vmId, poolId);

        // 使用新线程调用 flowLogAndAppLogOverviewDataES 方法，并校验（日志在方法内打印）
        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【overview|流量】 开始调用 flowLogAndAppLogOverviewDataES 旧方法验证 ---------->");
                try {
                    flowLogAndAppLogOverviewDataES(tenantOrgCode, logStartTime, logEndTime, isInternal, vmId);
                } catch (Exception e) {
                    logger.error("捕获小异常一只，堆栈信息如下: ", e);
                }
            }
        });

        return ckRes;
    }

    @Deprecated
    private OverviewInfo flowLogAndAppLogOverviewDataES(String tenantOrgCode, Date logStartTime, Date logEndTime, Boolean isInternal, Long vmId) throws Exception {
        OverviewInfo overviewInfo = new OverviewInfo();
        Long poolId = null;
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = vMwareDevice.getPoolId();
        }

        NetWorkWebSearchDTO netWorkWebSearchDTO = new NetWorkWebSearchDTO();
        List<WebAppBean> appList = new ArrayList<>();
        List<WebIpBean> ipList = new ArrayList<>();

        if (DataUtils.isNotEmpty(tenantOrgCode)){
            netWorkWebSearchDTO.setTenant_org_code(tenantOrgCode);
        }
        if (logStartTime != null){
            String startTime = DateUtils.format(logStartTime, DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            netWorkWebSearchDTO.setStartTime(startTime);
        }
        if (logEndTime != null){
            String endTime = DateUtils.format(logEndTime, DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            netWorkWebSearchDTO.setEndTime(endTime);
        }
        if (vmId != null){
            netWorkWebSearchDTO.setVmId("" + vmId);
        }
        if(!isInternal){
            netWorkWebSearchDTO.setPool_id(String.valueOf(poolId));
        }
        logger.info("查询网审IP流量、应用流量日志请求参数信息:{}", JSON.toJSONString(netWorkWebSearchDTO));
        NetWorkWebDataBean response = netWorkSearchDubboService.queryWebData(netWorkWebSearchDTO);
        logger.info("查询网审IP流量、应用流量日志响应信息:{}", JSON.toJSONString(response));
        Assert.notNull(response, "[接口调用异常,请检查调用参数]查询网审IP流量、应用流量日志响应信息为空");
        appList = response.getApp_list();
        ipList = response.getIp_list();

        List<NetworkAuditOverviewAppLogItem> appLog = appList.stream().map(l-> new NetworkAuditOverviewAppLogItem(l.getNameCn(), l.getTotalBytes())).collect(Collectors.toList());
        List<NetworkAuditOverviewFlowLogItem> flowLog = ipList.stream().map(l-> new NetworkAuditOverviewFlowLogItem(l.getNameCn(), l.getDownBytes(), l.getUpBytes(), l.getTotalBytes())).collect(Collectors.toList());

        overviewInfo.setAppLog(appLog);
        overviewInfo.setFlowLog(flowLog);

        logger.info("【overview|流量】 旧方法验证结束 ----------> {}", JSON.toJSONString(overviewInfo));
        return overviewInfo;
    }

    @Override
    public List<SecurityEventInfo> securityOverviewData(String tenantOrgCode, Date logStartTime, Date logEndTime, Boolean isInternal, Long vmId) throws Exception {
        String poolId = "";
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = DataUtils.isNotEmpty(vMwareDevice.getPoolId()) ? String.valueOf(vMwareDevice.getPoolId()) : poolId;
        }
        List<SecurityEventInfo> ckRes = networkSecuritylogEventClickhouseService.overviewQuery(tenantOrgCode, logStartTime, logEndTime, vmId, poolId);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【overview|安全】 开始调用 securityOverviewDataES 旧方法验证 ---------->");
                try {
                    securityOverviewDataES(tenantOrgCode, logStartTime, logEndTime, isInternal, vmId);
                } catch (Exception e) {
                    logger.error("捕获小异常一只，堆栈信息如下: ", e);
                }
            }
        });

        return ckRes;
    }

    @Deprecated
    private List<SecurityEventInfo> securityOverviewDataES(String tenantOrgCode, Date logStartTime, Date logEndTime, Boolean isInternal, Long vmId) throws Exception {
        List<SecurityEventInfo> overviewInfo = new LinkedList<>();
        Long poolId = null;
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = vMwareDevice.getPoolId();
        }

        NetworkSecurityStatisticsDTO networkSecurityStatisticsDTO =  new NetworkSecurityStatisticsDTO();

        if (DataUtils.isNotEmpty(tenantOrgCode)){
            networkSecurityStatisticsDTO.setTenantOrgCode(tenantOrgCode);
        }
        if (logStartTime != null){
            String startTime = DateUtils.format(logStartTime, DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            networkSecurityStatisticsDTO.setStartTime(startTime);
        }
        if (logEndTime != null){
            String endTime = DateUtils.format(logEndTime, DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
            networkSecurityStatisticsDTO.setEndTime(endTime);
        }
        if (vmId != null){
            networkSecurityStatisticsDTO.setVmId("" + vmId);
        }
        if(!isInternal){
            networkSecurityStatisticsDTO.setPoolId(String.valueOf(poolId));
        }
        logger.info("查询网审IP流量、应用流量日志请求参数信息:{}", JSON.toJSONString(networkSecurityStatisticsDTO));


        SecurityBean securityBean = netWorkSearchDubboService.SecurityQuery(networkSecurityStatisticsDTO);
        if(securityBean == null){
            securityBean = new SecurityBean();
        }
        //造
        SecurityEventInfo s1 = new SecurityEventInfo();
        s1.setEventName("入侵防御");
        s1.setNum(securityBean.getIpsCount() == null ? 0 : Long.valueOf(securityBean.getIpsCount()));
        SecurityEventInfo s2 = new SecurityEventInfo();
        s2.setEventName("病毒防护");
        s2.setNum(securityBean.getAvCount() == null ? 0 : Long.valueOf(securityBean.getAvCount()));
        SecurityEventInfo s3 = new SecurityEventInfo();
        s3.setEventName("恶意url检测");
        s3.setNum(securityBean.getMalwareAppCount() == null ? 0 : Long.valueOf(securityBean.getMalwareAppCount()));
        overviewInfo.add(s1);
        overviewInfo.add(s2);
        overviewInfo.add(s3);

        logger.info("【overview|安全】 旧方法验证结束 ----------> {}", JSON.toJSONString(overviewInfo));
        return overviewInfo;
    }

    @Override
    public NetworkAuditUserLogDTO userLogPage(String tenantOrgCode, NetworkAuditUserLogDO networkAuditUserLogDO, Integer current, Integer size, Boolean isInternal) throws Exception {
        NetworkAuditUserLogDTO ckRes = networkUserlogEventClickhouseService.specifiedPage(tenantOrgCode, networkAuditUserLogDO, current, size);

        // 使用新线程调用 userLogPageES 方法，并校验（日志在方法内打印）
        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【page|行为】 开始调用 userLogPageES 旧方法验证 ---------->");
                try {
                    userLogPageES(tenantOrgCode, networkAuditUserLogDO, current, size, isInternal);
                } catch (Exception e) {
                    logger.error("捕获小异常一只，堆栈信息如下: ", e);
                }
            }
        });

        return ckRes;
    }

    @Deprecated
    private NetworkAuditUserLogDTO userLogPageES(String tenantOrgCode, NetworkAuditUserLogDO networkAuditUserLogDO, Integer current, Integer size, Boolean isInternal) throws Exception {
        Long poolId = null;
        if (!isInternal) {
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = vMwareDevice.getPoolId();
        }

        List<NetworkAuditUserLogDO> resultList = new LinkedList<>();
        NetworkAuditUserLogDTO logObject = new NetworkAuditUserLogDTO();

//        Calendar calStart = Calendar.getInstance();
//        Calendar calEnd = Calendar.getInstance();

//        if (networkAuditUserLogDO.getStartTime() != null && networkAuditUserLogDO.getEndTime() != null) {
//            calStart.setTime(networkAuditUserLogDO.getStartTime());
//            calEnd.setTime(networkAuditUserLogDO.getEndTime());
//            calStart.add(Calendar.DATE, 30);

//            if (calEnd.after(calStart)) {
//                //超过一个月
                //全部通过es查询
                NetWorkDataSearchDTO netWorkDataSearchDTO = new NetWorkDataSearchDTO();
                netWorkDataSearchDTO.setPageNum(current);
                netWorkDataSearchDTO.setPageSize(size);
                if (DataUtils.isNotEmpty(tenantOrgCode)) {
                    netWorkDataSearchDTO.setTenant_org_code(tenantOrgCode);
                }
                if (networkAuditUserLogDO.getStartTime() != null) {
                    netWorkDataSearchDTO.setStartTime(DateUtils.format(networkAuditUserLogDO.getStartTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS));
                }
                if (networkAuditUserLogDO.getEndTime() != null) {
                    netWorkDataSearchDTO.setEndTime(DateUtils.format(networkAuditUserLogDO.getEndTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS));
                }
                if (DataUtils.isNotEmpty(networkAuditUserLogDO.getLog_type())) {
                    netWorkDataSearchDTO.setLog_type(networkAuditUserLogDO.getLog_type());
                }
                if (DataUtils.isNotEmpty(networkAuditUserLogDO.getSrc_ip())) {
                    netWorkDataSearchDTO.setSrc_ip(networkAuditUserLogDO.getSrc_ip());
                }
                if (DataUtils.isNotEmpty(networkAuditUserLogDO.getDst_ip())) {
                    netWorkDataSearchDTO.setDst_ip(networkAuditUserLogDO.getDst_ip());
                }
                if (DataUtils.isNotEmpty(networkAuditUserLogDO.getOrderBy())) {
                    netWorkDataSearchDTO.setSort(networkAuditUserLogDO.getOrderBy().replace(" ", ":"));
                } else {
                    netWorkDataSearchDTO.setSort("log_time desc".replace(" ", ":"));
                }

                logger.info("查询网审用户日志请求参数信息:{}", JSON.toJSONString(netWorkDataSearchDTO));
                DataBean<UserBean> response = netWorkSearchDubboService.pagingQuery(netWorkDataSearchDTO);
                logger.info("查询网审用户日志请求响应信息:{}", JSON.toJSONString(response));
                Assert.notNull(response, "[接口调用异常，请检查调用参数]查询网审用户日志响应为空");

                List<UserBean> data = response.getDatas();
                if (DataUtils.isEmpty(data)) {
                    logger.info("网审用户日志为空");
                }
                Long total = response.getTotal();
                for (UserBean log : data) {
                    String json = JSON.toJSONString(log);
                    NetworkAuditUserLogDO logDO = JSON.parseObject(json, NetworkAuditUserLogDO.class);
                    resultList.add(logDO);
                }
                logObject.setTotal(total);
                logObject.setData(resultList);
//            } else {
//                //一个月内
//                NetworkUserlogEventDO networkUserlogEventDO = buildNetworkUserlogEventDO(tenantOrgCode, networkAuditUserLogDO);
//                logger.info("mysql查询网审用户日志请求参数信息:{}", JSON.toJSONString(networkUserlogEventDO));
//                Pagination<NetworkUserlogEventDO> pagination = networkUserlogEventService.page(networkUserlogEventDO, new Pagination<>(current, size));
//                logObject.setTotal(pagination.getTotal());
//                List<NetworkAuditUserLogDO> networkAuditUserLogDOS = convertToNetworkAuditUserLogDOList(pagination.getRecords());
//                logObject.setData(networkAuditUserLogDOS);
//            }
//        } else {
//            //不选时间默认查mysql
//            NetworkUserlogEventDO networkUserlogEventDO = buildNetworkUserlogEventDO(tenantOrgCode, networkAuditUserLogDO);
//            logger.info("mysql查询网审用户日志请求参数信息:{}", JSON.toJSONString(networkUserlogEventDO));
//            Pagination<NetworkUserlogEventDO> pagination = networkUserlogEventService.page(networkUserlogEventDO, new Pagination<>(current, size));
//            logObject.setTotal(pagination.getTotal());
//            List<NetworkAuditUserLogDO> networkAuditUserLogDOS = convertToNetworkAuditUserLogDOList(pagination.getRecords());
//            logObject.setData(networkAuditUserLogDOS);
//        }

        logger.info("【page|行为】 旧方法验证结束 ----------> {}", JSON.toJSONString(logObject));
        return logObject;
    }

    private NetworkUserlogEventDO buildNetworkUserlogEventDO(String tenantOrgCode, NetworkAuditUserLogDO networkAuditUserLogDO) {
        NetworkUserlogEventDO networkUserlogEventDO = new NetworkUserlogEventDO();
        if (DataUtils.isNotEmpty(tenantOrgCode)) {
            networkUserlogEventDO.setTenantOrgCode(tenantOrgCode);
        }
        if (networkAuditUserLogDO.getStartTime() != null) {
            networkUserlogEventDO.setStartTime(networkAuditUserLogDO.getStartTime());
        }
        if (networkAuditUserLogDO.getEndTime() != null) {
            networkUserlogEventDO.setEndTime(networkAuditUserLogDO.getEndTime());
        }
        if (DataUtils.isNotEmpty(networkAuditUserLogDO.getLog_type())) {
            networkUserlogEventDO.setLogType(networkAuditUserLogDO.getLog_type());
        }
        if (DataUtils.isNotEmpty(networkAuditUserLogDO.getSrc_ip())) {
            networkUserlogEventDO.setSrcIp(networkAuditUserLogDO.getSrc_ip());
        }
        if (DataUtils.isNotEmpty(networkAuditUserLogDO.getDst_ip())) {
            networkUserlogEventDO.setDstIp(networkAuditUserLogDO.getDst_ip());
        }
        if (DataUtils.isNotEmpty(networkAuditUserLogDO.getOrderBy())) {
            networkUserlogEventDO.setOrderBy(networkAuditUserLogDO.getOrderBy());
        } else {
            networkUserlogEventDO.setOrderBy("log_time desc");
        }
        if (DataUtils.isNotEmpty(networkAuditUserLogDO.getPool_id())) {
            networkUserlogEventDO.setPoolId(networkAuditUserLogDO.getPool_id());
        }
        return networkUserlogEventDO;
    }

    @Override
    public NetworkAuditSecurityLogDTO securityLogPage(String tenantOrgCode, NetworkAuditSecurityLogDO networkAuditSecurityLogDO, Integer current, Integer size, Boolean isInternal) throws Exception {
        NetworkAuditSecurityLogDTO ckRes = networkSecuritylogEventClickhouseService.specifiedPage(tenantOrgCode, networkAuditSecurityLogDO, current, size);

        // 使用新线程调用 securityLogPageES 方法，并校验（日志在方法内打印）
        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【page|安全】 开始调用 securityLogPageES 旧方法验证 ---------->");
                try {
                    securityLogPageES(tenantOrgCode, networkAuditSecurityLogDO, current, size, isInternal);
                } catch (Exception e) {
                    logger.error("捕获小异常一只，堆栈信息如下: ", e);
                }
            }
        });

        return ckRes;
    }

    @Deprecated
    private NetworkAuditSecurityLogDTO securityLogPageES(String tenantOrgCode, NetworkAuditSecurityLogDO networkAuditSecurityLogDO, Integer current, Integer size, Boolean isInternal) throws Exception {

        Long poolId = null;
        if(!isInternal){
            VMwareDevice vMwareDevice = getVMwareDevice(tenantOrgCode);
            poolId = vMwareDevice.getPoolId();
        }

        List<NetworkAuditSecurityLogDO> resultList = new LinkedList<>();
        NetworkAuditSecurityLogDTO logObject = new NetworkAuditSecurityLogDTO();

//        Calendar calStart = Calendar.getInstance();
//        Calendar calEnd = Calendar.getInstance();

//        if (networkAuditSecurityLogDO.getStartTime() != null && networkAuditSecurityLogDO.getEndTime() != null) {
//            calStart.setTime(networkAuditSecurityLogDO.getStartTime());
//            calEnd.setTime(networkAuditSecurityLogDO.getEndTime());
//            calStart.add(Calendar.DATE, 30);

//            if (calEnd.after(calStart)) {
//                //超过一个月
                //全部通过es查询
                NetWorkDataSearchDTO netWorkDataSearchDTO = new NetWorkDataSearchDTO();
                netWorkDataSearchDTO.setPageNum(current);
                netWorkDataSearchDTO.setPageSize(size);
                if (DataUtils.isNotEmpty(tenantOrgCode)){
                    netWorkDataSearchDTO.setTenant_org_code(tenantOrgCode);
                }
                if (networkAuditSecurityLogDO.getStartTime() != null){
                    String startTime = DateUtils.format(networkAuditSecurityLogDO.getStartTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
                    netWorkDataSearchDTO.setStartTime(startTime);
                }
                if (networkAuditSecurityLogDO.getEndTime() != null){
                    String endTime = DateUtils.format(networkAuditSecurityLogDO.getEndTime(), DateUtils.DatePattern.YYYY_MM_DD_HH_MM_SS);
                    netWorkDataSearchDTO.setEndTime(endTime);
                }
                if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getLog_type())){
                    netWorkDataSearchDTO.setLog_type(networkAuditSecurityLogDO.getLog_type());
                }
                if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getSrc_ip())){
                    netWorkDataSearchDTO.setSrc_ip(networkAuditSecurityLogDO.getSrc_ip());
                }
                if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getDst_ip())){
                    netWorkDataSearchDTO.setDst_ip(networkAuditSecurityLogDO.getDst_ip());
                }
                if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getOrderBy())){
                    netWorkDataSearchDTO.setSort(networkAuditSecurityLogDO.getOrderBy().replace(" ", ":"));
                }else{
                    netWorkDataSearchDTO.setSort("ctime desc".replace(" ", ":"));
                }

                logger.info("查询网审安全日志请求参数信息:{}", JSON.toJSONString(netWorkDataSearchDTO));
                DataBean<securityPagingBean> response = netWorkSearchDubboService.pagingSecurityPagingQuery(netWorkDataSearchDTO);
                logger.info("查询网审安全日志请求响应信息:{}", JSON.toJSONString(response));
                Assert.notNull(response, "[接口调用异常，请检查调用参数]");
                List<securityPagingBean> data = response.getDatas();
                if (DataUtils.isEmpty(data)){
                    logger.info("网审安全日志为空");
                }
                Long total = response.getTotal();
                for (securityPagingBean log : data) {
                    String json = JSON.toJSONString(log);
                    NetworkAuditSecurityLogDO logDO = JSON.parseObject(json, NetworkAuditSecurityLogDO.class);
                    logDO.setLog_time(log.getCtime());
                    logDO.setHigh_level_protocol_type(log.getApp_protocol());
                    logDO.setProtocol_type(log.getProtocol());
                    logDO.setLog_level(log.getLevel());
                    resultList.add(logDO);
                }

                logObject.setTotal(total);
                logObject.setData(resultList);
//            }else{
//                //一个月内
//                NetworkSecurityEventDO networkSecurityEventDO = buildNetworkSecurityEventDO(tenantOrgCode, networkAuditSecurityLogDO);
//                logger.info("mysql查询网审安全日志请求参数信息:{}", JSON.toJSONString(networkSecurityEventDO));
//                Pagination<NetworkSecurityEventDO> pagination = networkSecurityEventService.page(networkSecurityEventDO, new Pagination<>(current, size));
//                logObject.setTotal(pagination.getTotal());
//                List<NetworkAuditSecurityLogDO> networkAuditSecurityLogDOS = convertToNetworkAuditSecurityLogDOList(pagination.getRecords());
//                logObject.setData(networkAuditSecurityLogDOS);
//            }
////        }else{
//            //不选时间默认查mysql
//            NetworkSecurityEventDO networkSecurityEventDO = buildNetworkSecurityEventDO(tenantOrgCode, networkAuditSecurityLogDO);
//            logger.info("mysql查询网审用户日志请求参数信息:{}", JSON.toJSONString(networkSecurityEventDO));
//            Pagination<NetworkSecurityEventDO> pagination = networkSecurityEventService.page(networkSecurityEventDO, new Pagination<>(current, size));
//            logObject.setTotal(pagination.getTotal());
//            List<NetworkAuditSecurityLogDO> networkAuditSecurityLogDOS = convertToNetworkAuditSecurityLogDOList(pagination.getRecords());
//            logObject.setData(networkAuditSecurityLogDOS);
//        }

        logger.info("【page|安全】 旧方法验证结束 ----------> {}", JSON.toJSONString(logObject));
        return logObject;
    }

    private NetworkSecurityEventDO buildNetworkSecurityEventDO(String tenantOrgCode, NetworkAuditSecurityLogDO networkAuditSecurityLogDO) {
        NetworkSecurityEventDO NetworkSecurityEventDO = new NetworkSecurityEventDO();
        if (DataUtils.isNotEmpty(tenantOrgCode)) {
            NetworkSecurityEventDO.setTenantOrgCode(tenantOrgCode);
        }
        if (networkAuditSecurityLogDO.getStartTime() != null) {
            NetworkSecurityEventDO.setStartTime(networkAuditSecurityLogDO.getStartTime());
        }
        if (networkAuditSecurityLogDO.getEndTime() != null) {
            NetworkSecurityEventDO.setEndTime(networkAuditSecurityLogDO.getEndTime());
        }
        if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getLog_type())) {
            NetworkSecurityEventDO.setLogType(networkAuditSecurityLogDO.getLog_type());
        }
        if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getSrc_ip())) {
            NetworkSecurityEventDO.setSrcIp(networkAuditSecurityLogDO.getSrc_ip());
        }
        if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getDst_ip())) {
            NetworkSecurityEventDO.setDstIp(networkAuditSecurityLogDO.getDst_ip());
        }
        if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getOrderBy())) {
            NetworkSecurityEventDO.setOrderBy(networkAuditSecurityLogDO.getOrderBy());
        } else {
            NetworkSecurityEventDO.setOrderBy("ctime desc");
        }
        if (DataUtils.isNotEmpty(networkAuditSecurityLogDO.getPool_id())) {
            NetworkSecurityEventDO.setPoolId(networkAuditSecurityLogDO.getPool_id());
        }
        return NetworkSecurityEventDO;
    }


    public  List<NetworkAuditUserLogDO> convertToNetworkAuditUserLogDOList(List<NetworkUserlogEventDO> userlogEventDOList) {
        List<NetworkAuditUserLogDO> auditUserLogDOList = new ArrayList<>();
        for (NetworkUserlogEventDO eventDO : userlogEventDOList) {
            NetworkAuditUserLogDO auditLogDO = new NetworkAuditUserLogDO();

            auditLogDO.setAccount(eventDO.getAccount());
            auditLogDO.setAction_name(eventDO.getActionName());
            auditLogDO.setApp_cat_name(eventDO.getAppCatName());
            auditLogDO.setApp_name(eventDO.getAppName());
            auditLogDO.setContent(eventDO.getContent());
            auditLogDO.setDst_ip(eventDO.getDstIp());
            auditLogDO.setDst_port(eventDO.getDstPort());
            auditLogDO.setFile_name(eventDO.getFileName());
            auditLogDO.setFile_size(eventDO.getFileSize());
            auditLogDO.setHandle_action(eventDO.getHandleAction());
            auditLogDO.setLog_time(eventDO.getLogTime());
            auditLogDO.setLog_type(eventDO.getLogType());
            auditLogDO.setMsg(eventDO.getMsg());
            auditLogDO.setPid(eventDO.getPid());
            auditLogDO.setPool_id(eventDO.getPoolId());
            auditLogDO.setReceive_addr(eventDO.getReceiveAddr());
            auditLogDO.setSend_addr(eventDO.getSendAddr());
            auditLogDO.setSrc_ip(eventDO.getSrcIp());
            auditLogDO.setSrc_mac(eventDO.getSrcMac());
            auditLogDO.setSubject(eventDO.getSubject());
            auditLogDO.setTenant_org_code(eventDO.getTenantOrgCode());
            auditLogDO.setTerm_device(eventDO.getTermDevice());
            auditLogDO.setTerm_platform(eventDO.getTermPlatform());
            auditLogDO.setUrl(eventDO.getUrl());
            auditLogDO.setUrl_cate_name(eventDO.getUrlCateName());
            auditLogDO.setUrl_domain(eventDO.getUrlDomain());
            auditLogDO.setUser_group_name(eventDO.getUserGroupName());
            auditLogDO.setUser_name(eventDO.getUserName());
            auditLogDO.setUuid(eventDO.getUuid());
            auditLogDO.setStartTime(eventDO.getStartTime());
            auditLogDO.setEndTime(eventDO.getEndTime());

            auditUserLogDOList.add(auditLogDO);
        }
        return auditUserLogDOList;
    }

    public  List<NetworkAuditSecurityLogDO> convertToNetworkAuditSecurityLogDOList(List<NetworkSecurityEventDO> securityEventDOList) {
        List<NetworkAuditSecurityLogDO> auditSecurityLogDOList = new ArrayList<>();
        for (NetworkSecurityEventDO eventDO : securityEventDOList) {
            NetworkAuditSecurityLogDO auditLogDO = new NetworkAuditSecurityLogDO();

            auditLogDO.setLog_type(eventDO.getLogType());
            auditLogDO.setUser_id(eventDO.getUserId());
            auditLogDO.setUser_name(eventDO.getUserName());
            auditLogDO.setPolicy_id(eventDO.getPolicyId());
            auditLogDO.setSrc_mac(eventDO.getSrcMac());
            auditLogDO.setDst_mac(eventDO.getDstMac());
            auditLogDO.setSrc_ip(eventDO.getSrcIp());
            auditLogDO.setDst_ip(eventDO.getDstIp());
            auditLogDO.setSrc_port(eventDO.getSrcPort() != null ? Integer.valueOf(eventDO.getSrcPort()) : null);
            auditLogDO.setDst_port(eventDO.getDstPort() != null ? Integer.valueOf(eventDO.getDstPort()) : null);
            auditLogDO.setApp_name(eventDO.getAppName());
            auditLogDO.setProtocol(eventDO.getProtocol());
            auditLogDO.setHigh_level_protocol_type(eventDO.getAppProtocol());
            auditLogDO.setProtocol_type(eventDO.getProtocol());
            auditLogDO.setLog_level(eventDO.getLevel());
            // 日期转换
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String formattedDate = sdf.format(eventDO.getCtime());
            auditLogDO.setLog_time(formattedDate);
            auditLogDO.setApp_protocol(eventDO.getAppProtocol());
            auditLogDO.setEvent_id(eventDO.getEventId());
            auditLogDO.setEvent_name(eventDO.getEventName());
            auditLogDO.setEvent_type(eventDO.getEventType());
            auditLogDO.setAgg_mode(eventDO.getAggMode() != null ? Integer.valueOf(eventDO.getAggMode()) : null);
            auditLogDO.setAgg_count(eventDO.getAggCount() != null ? Integer.valueOf(eventDO.getAggCount()) : null);
            auditLogDO.setAttack_success(eventDO.getAttackSuccess() != null ? Integer.valueOf(eventDO.getAttackSuccess()) : null);
            auditLogDO.setLevel(eventDO.getLevel());
            auditLogDO.setAction_name(eventDO.getAction());
            auditLogDO.setVirus_name(eventDO.getVirusName());
            auditLogDO.setFile_name(eventDO.getFileName());
            auditLogDO.setApp_name_en(eventDO.getAppNameEn());
            auditLogDO.setUrl(eventDO.getUrl());
            auditLogDO.setMsg(eventDO.getMsg());
            auditLogDO.setTenant_org_code(eventDO.getTenantOrgCode());
            auditLogDO.setTerm_device(eventDO.getTermDevice());
            auditLogDO.setTerm_platform(eventDO.getTermPlatform());
            auditLogDO.setWeb_name(eventDO.getWebName());
            auditLogDO.setUser_group_name(eventDO.getUserGroupName());
            auditLogDO.setPool_id(eventDO.getPoolId());

            auditSecurityLogDOList.add(auditLogDO);
        }
        return auditSecurityLogDOList;
    }

    private VMwareDevice getVMwareDevice(String tenantOrgCode) throws Exception {
        logger.info("查询租户资产参数:{}", JSON.toJSONString(tenantOrgCode));
        List<AssetDO> assetDOList = tOrderDubboService.getServingAsset(tenantOrgCode, true);
        logger.info("资产列表{}", JSON.toJSONString(assetDOList));
        VMDO vmdo = null;
        outer:for (AssetDO assetDO : assetDOList) {
            List<TOrderServiceAbilityDO> abilitys = assetDO.getAbilitys();
            for (TOrderServiceAbilityDO ability : abilitys) {
                if (ability.getAbilityId() == 6){
                    Long id = assetDO.getId();
                    vmdo = assetVmInfoDubboService.getVmByAssetService(id, 1, 6L);
                    break outer;
                }
            }
        }
        Assert.notNull(vmdo,"该资产下没有与之关联的网络安全审计原子能力");
        return powerService.getDeviceInfo(vmdo.getId());
    }

//    private List<?> trans(Map<String, String> param, Long poolId, Class<?> cls) throws Exception {
//        String data = null;
//        try {
//            String query = esNetworkAuditLogDubboService.netPagingQuery(param, poolId);
//            JSONObject jsonObject = JSONObject.parseObject(query);
//            String statusCode = jsonObject.getString("status_code");
//            String msg = jsonObject.getString("msg");
//            if (!"200".equals(statusCode)){
//                logger.info("查询网络安全审计日志失败status_code:[{}], msg:[{}]", statusCode,  msg);
//            }
//            data = jsonObject.getString("data");
//        }catch (Exception e){
//            e.printStackTrace();
//            throw new GenericException(HttpCode.INTERNAL_SERVER_ERROR, "查询日志失败");
//        }
//
//        if (cls == NetworkAuditFlowLogV6DO.class){
//            return JSON.parseArray(data, NetworkAuditFlowLogV6DO.class);
//        }
//        else if (cls == NetworkAuditUserLogDO.class){
//            return JSON.parseArray(data, NetworkAuditUserLogDO.class);
//        }
//        else {
//            return JSON.parseArray(data, NetworkAuditHealthLogDO.class);
//        }
//    }


    @Override
    public Map<Long, Long> getUsedBandwidth(List<Long> vmIds) {
        List<FlowStatDO> usedBandwidth = flowStatService.getUsedBandwidth(vmIds);
        Map<Long, Long> collect = usedBandwidth.stream().collect(Collectors.toMap(FlowStatDO::getVmId, FlowStatDO::getAllTotalBytes));
        return collect;
    }
}
