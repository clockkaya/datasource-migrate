package com.sama.networkaudit.service.impl;

import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.base.nokey.impl.NkBaseServiceImpl;
import com.core4ct.support.Pagination;
import com.core4ct.utils.DataUtils;
import com.core4ct.utils.DateUtils;
import com.core4ct.utils.PageUtils;
import com.sama.api.networkaudit.bean.*;
import com.sama.networkaudit.mapper.clickhouse.NetworkFlowlogEventClickhouseMapper;
import com.sama.networkaudit.service.NetworkFlowlogEventClickhouseService;
import com.sama.networkaudit.utils.CommonUtils;
import com.sama.networkaudit.utils.StringToDateConverter;
import com.sama.networkaudit.utils.data.AssetInfoUtils;
import com.sama.networkaudit.utils.data.bean.FlowLogBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.modelmapper.ModelMapper;
import org.modelmapper.PropertyMap;
import org.modelmapper.convention.MatchingStrategies;
import org.modelmapper.convention.NameTokenizers;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/14 13:45
 */
@Service
public class NetworkFlowlogEventClickhouseServiceImpl
        extends NkBaseServiceImpl<NetworkFlowlogEventClickhouseDO, NetworkFlowlogEventClickhouseMapper>
        implements NetworkFlowlogEventClickhouseService {

    private final static Logger logger = LogManager.getLogger(NetworkFlowlogEventClickhouseService.class);

    private final ModelMapper migrationMapper;

    @Resource
    AssetInfoUtils assetInfoUtils;

    public NetworkFlowlogEventClickhouseServiceImpl() {
        super();
        // 比较简单的初始化在构建函数完成
        this.migrationMapper = new ModelMapper();
        this.migrationMapper.getConfiguration()
                .setSourceNameTokenizer(NameTokenizers.UNDERSCORE)
                .setDestinationNameTokenizer(NameTokenizers.CAMEL_CASE)
                .setMatchingStrategy(MatchingStrategies.STANDARD)
                .setAmbiguityIgnored(true);

        // 添加自定义日期转换器
        this.migrationMapper.createTypeMap(String.class, Date.class).setConverter(new StringToDateConverter("yyyy-MM-dd HH:mm:ss"));

        // 忽略 createTime 属性
        this.migrationMapper.addMappings(new PropertyMap<FlowLogBean, NetworkFlowlogEventClickhouseDO>() {
            @Override
            protected void configure() {
                skip(destination.getCreateTime());
            }
        });
    }

    @Override
    public void batchInsert(List<NetworkFlowlogEventClickhouseDO> list) {
        this.mapper.batchInsert(list);
    }

    @Override
    public void batchMigrate(List<FlowLogBean> rawList) {
        List<NetworkFlowlogEventClickhouseDO> targetList = new ArrayList<>();
        rawList.forEach(flowLogBean -> {
            NetworkFlowlogEventClickhouseDO flowlogClickhouseDO = migrationMapper.map(flowLogBean, NetworkFlowlogEventClickhouseDO.class);
            // 补充vm信息
            String vmName = assetInfoUtils.getVmName(flowLogBean.getPool_org_code(), flowLogBean.getVm_ip());
            flowlogClickhouseDO.setVmName(vmName);
            flowlogClickhouseDO.setCreateTime(new Date());
            flowlogClickhouseDO.setUpdateTime(new Date());
            targetList.add(flowlogClickhouseDO);
        });
        logger.info("【迁移|流量】 List<FlowLogBean> -----> List<NetworkFlowlogEventClickhouseDO>\n{}\n-----> {}",
                JSON.toJSONString(rawList), JSON.toJSONString(targetList));
        batchInsert(targetList);
    }

    @Override
    public List<NetworkAuditFlowLogV6DO> specifiedPage(String tenantOrgCode, NetworkAuditFlowLogV6DO inputParams, Integer current, Integer size, Long vmId, String poolId) {
        NetworkFlowlogEventClickhouseDO queryDO = new NetworkFlowlogEventClickhouseDO();
        queryDO.setVmId(String.valueOf(vmId));
        queryDO.setTenantOrgCode(tenantOrgCode);
        queryDO.setLogStartTime(inputParams.getStartTime());
        queryDO.setLogEndTime(inputParams.getEndTime());
        queryDO.setLogTypes(CommonUtils.convertStringToList(inputParams.getLogType()));
        queryDO.setPoolId(poolId);
        if (DataUtils.isNotEmpty(inputParams.getOrderBy())) {
            queryDO.setOrderBy(inputParams.getOrderBy());
        } else {
            queryDO.setOrderBy("log_time desc");
        }
        logger.info("【page|流量】 请求 clickhouse 参数信息 NetworkFlowlogEventClickhouseDO: {}", JSON.toJSONString(queryDO));

        Pagination<NetworkFlowlogEventClickhouseDO> rowBounds = new Pagination<>(current, size);
        Page<NetworkFlowlogEventClickhouseDO> page = PageUtils.getPage(rowBounds);
        List<NetworkFlowlogEventClickhouseDO> rawRes = this.mapper.listFlowTrafficGroupByTime(queryDO);
        logger.info("【page|流量】 从 clickhouse 直接查询结果: {}", JSON.toJSONString(rawRes));

        //这里在时间间隔内插入0值流量（up/down）按每分钟统计
        Map<Date, Long> map_up = rawRes.stream().collect(Collectors.toMap(NetworkFlowlogEventClickhouseDO::getLogTime, NetworkFlowlogEventClickhouseDO::getUp));
        Map<Date, Long> map_down = rawRes.stream().collect(Collectors.toMap(NetworkFlowlogEventClickhouseDO::getLogTime, NetworkFlowlogEventClickhouseDO::getDown));
        List<String> logTimeList = this.getTimeXaxisPreMinute( inputParams.getStartTime(), inputParams.getEndTime());
        List<NetworkFlowlogEventClickhouseDO> rawResPerMinute=new ArrayList<>();
        for (String logTime : logTimeList) {
            NetworkFlowlogEventClickhouseDO networkFlowlogEventClickhouseDO = new NetworkFlowlogEventClickhouseDO();
            networkFlowlogEventClickhouseDO.setLogTime(DateUtils.stringToDate(logTime));
            networkFlowlogEventClickhouseDO.setUp(map_up.getOrDefault(DateUtils.stringToDate(logTime), 0L));
            networkFlowlogEventClickhouseDO.setDown(map_down.getOrDefault(DateUtils.stringToDate(logTime), 0L));
            rawResPerMinute.add(networkFlowlogEventClickhouseDO);
        }
        List<NetworkAuditFlowLogV6DO> targetDataList = new ArrayList<>();
        rawResPerMinute.forEach(flowlogClickhouseDO -> {
            NetworkAuditFlowLogV6DO targetDO = new NetworkAuditFlowLogV6DO();
            BeanUtils.copyProperties(flowlogClickhouseDO, targetDO);
            targetDO.setLogTime(DateUtils.format(flowlogClickhouseDO.getLogTime(), "yyyy-MM-dd HH:mm:ss"));
            targetDataList.add(targetDO);
        });
        logger.info("【page|流量】 最终格式 List<NetworkAuditFlowLogV6DO>: {}", JSON.toJSONString(targetDataList));

        return targetDataList;
    }

    @Override
    public OverviewInfo overviewQuery(String tenantOrgCode, Date logStartTime, Date logEndTime, Long vmId, String poolId) {
        NetworkFlowlogEventClickhouseDO queryDO = new NetworkFlowlogEventClickhouseDO();
        queryDO.setTenantOrgCode(tenantOrgCode);
        queryDO.setLogStartTime(logStartTime);
        queryDO.setLogEndTime(logEndTime);
        queryDO.setVmId(String.valueOf(vmId));
        queryDO.setPoolId(poolId);
        logger.info("【overview|流量】 请求 clickhouse 参数信息 NetworkFlowlogEventClickhouseDO: {}", JSON.toJSONString(queryDO));

        OverviewInfo targetDTO = new OverviewInfo();
        List<NetworkAuditOverviewAppLogItem> appnameGroup = this.mapper.listGroupByAppname(queryDO);
        List<NetworkAuditOverviewFlowLogItem> uipGroup = this.mapper.listGroupByUip(queryDO);
        // 这里直接返回了最终格式，不用多余的打印
        targetDTO.setAppLog(appnameGroup);
        targetDTO.setFlowLog(uipGroup);
        logger.info("【overview|流量】 最终格式 OverviewInfo: {}", JSON.toJSONString(targetDTO));

        return targetDTO;
    }

    private List<String> getTimeXaxisPreMinute(Date start, Date end) {
        if  (end == null) {
            end=new Date();
            if(start == null) {
                start = Date.from(end.toInstant().minus(1, ChronoUnit.HOURS));
            }
        }
        List<String> listDate = new ArrayList<>();
        listDate.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:00").format(start));
        Calendar calStart = Calendar.getInstance();
        calStart.setTime(start);
        Calendar calEnd = Calendar.getInstance();
        calEnd.setTime(end);
        while (calEnd.after(calStart)) {
                calStart.add(Calendar.MINUTE, 1);
                listDate.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:00").format(calStart.getTime()));
        }
        return listDate;
    }
}
