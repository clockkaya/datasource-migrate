package com.sama.networkaudit.service.impl;

import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.base.nokey.impl.NkBaseServiceImpl;
import com.core4ct.support.Pagination;
import com.core4ct.utils.DataUtils;
import com.core4ct.utils.PageUtils;
import com.sama.api.networkaudit.bean.NetworkAuditUserLogDO;
import com.sama.api.networkaudit.bean.NetworkAuditUserLogDTO;
import com.sama.api.networkaudit.bean.NetworkUserlogEventClickhouseDO;
import com.sama.networkaudit.mapper.clickhouse.NetworkUserlogEventClickhouseMapper;
import com.sama.networkaudit.service.NetworkUserlogEventClickhouseService;
import com.sama.networkaudit.utils.CommonUtils;
import com.sama.networkaudit.utils.DateToStringConverter;
import com.sama.networkaudit.utils.StringToDateConverter;
import com.sama.networkaudit.utils.data.AssetInfoUtils;
import com.sama.networkaudit.utils.data.bean.UserBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.modelmapper.convention.NameTokenizers;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/14 13:45
 */
@Service
public class NetworkUserlogEventClickhouseServiceImpl
        extends NkBaseServiceImpl<NetworkUserlogEventClickhouseDO, NetworkUserlogEventClickhouseMapper>
        implements NetworkUserlogEventClickhouseService {

    private final static Logger logger = LogManager.getLogger(NetworkUserlogEventClickhouseService.class);

    private final static String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * 迁移 mapper
     */
    private final ModelMapper migrationMapper;

    /**
     * 审计日志分页查询 mapper
     */
    private final ModelMapper specifiedPageMapper;

    @Resource
    AssetInfoUtils assetInfoUtils;

    public NetworkUserlogEventClickhouseServiceImpl() {
        super();
        // 比较简单的初始化在构建函数完成
        // hutool 的 BeanUtil.copyProperties 使用 CopyOptions 也可以达到类似功能，但是 ModelMapper 更加灵活
        this.migrationMapper = new ModelMapper();
        this.migrationMapper.getConfiguration()
                .setSourceNameTokenizer(NameTokenizers.UNDERSCORE)
                .setDestinationNameTokenizer(NameTokenizers.CAMEL_CASE)
                .setMatchingStrategy(MatchingStrategies.STANDARD);
        // 添加自定义日期转换器
        this.migrationMapper.createTypeMap(String.class, Date.class).setConverter(new StringToDateConverter(DATE_PATTERN));

        this.specifiedPageMapper = new ModelMapper();
        this.specifiedPageMapper.getConfiguration()
                .setSourceNameTokenizer(NameTokenizers.CAMEL_CASE)
                .setDestinationNameTokenizer(NameTokenizers.UNDERSCORE)
                .setMatchingStrategy(MatchingStrategies.STANDARD);
        this.specifiedPageMapper.createTypeMap(Date.class, String.class).setConverter(new DateToStringConverter(DATE_PATTERN));
        // 禁用自动映射
        this.specifiedPageMapper.getConfiguration().setAmbiguityIgnored(true);
    }

    @Override
    public void batchInsert(List<NetworkUserlogEventClickhouseDO> list) {
        this.mapper.batchInsert(list);
    }

    @Override
    public void batchMigrate(List<UserBean> rawList) {
        List<NetworkUserlogEventClickhouseDO> targetList = new ArrayList<>();
        rawList.forEach(userBean -> {
            NetworkUserlogEventClickhouseDO userlogClickhouseDO = migrationMapper.map(userBean, NetworkUserlogEventClickhouseDO.class);
            // 补充vm信息
            String vmName = assetInfoUtils.getVmName(userBean.getPool_org_code(), userBean.getVm_ip());
            userlogClickhouseDO.setVmName(vmName);
            userlogClickhouseDO.setCreateTime(new Date());
            userlogClickhouseDO.setUpdateTime(new Date());
            targetList.add(userlogClickhouseDO);
        });
        logger.info("【迁移|用户行为】 List<UserBean> -----> List<NetworkUserlogEventClickhouseDO>\n{}\n-----> {}",
                JSON.toJSONString(rawList), JSON.toJSONString(targetList));
        batchInsert(targetList);
    }

    @Override
    public NetworkAuditUserLogDTO specifiedPage(String tenantOrgCode, NetworkAuditUserLogDO inputParams, Integer current, Integer size) {
        NetworkUserlogEventClickhouseDO queryDO = new NetworkUserlogEventClickhouseDO();
        queryDO.setTenantOrgCode(tenantOrgCode);
        queryDO.setLogStartTime(inputParams.getStartTime());
        queryDO.setLogEndTime(inputParams.getEndTime());
        queryDO.setLogTypes(CommonUtils.convertStringToList(inputParams.getLog_type()));
        queryDO.setSrcIp(inputParams.getSrc_ip());
        queryDO.setDstIp(inputParams.getDst_ip());
        if (DataUtils.isNotEmpty(inputParams.getOrderBy())) {
            queryDO.setOrderBy(inputParams.getOrderBy());
        } else {
            queryDO.setOrderBy("log_time desc");
        }
        logger.info("【page|行为】 请求 clickhouse 参数信息 NetworkAuditUserLogDO: {}", JSON.toJSONString(queryDO));

        Pagination<NetworkUserlogEventClickhouseDO> rowBounds = new Pagination<>(current, size);
        Page<NetworkUserlogEventClickhouseDO> page = PageUtils.getPage(rowBounds);
        List<NetworkUserlogEventClickhouseDO> rawRes = this.mapper.pageByCondAndOrd(queryDO, page);
        logger.info("【page|行为】 从 clickhouse 直接查询结果: {}", JSON.toJSONString(rawRes));

        NetworkAuditUserLogDTO targetDTO = new NetworkAuditUserLogDTO();
        List<NetworkAuditUserLogDO> targetDataList = new ArrayList<>();
        rawRes.forEach(userlogClickhouseDO -> {
            NetworkAuditUserLogDO targetDO = specifiedPageMapper.map(userlogClickhouseDO, NetworkAuditUserLogDO.class);
            targetDataList.add(targetDO);
        });
        targetDTO.setTotal(page.getTotal());
        targetDTO.setData(targetDataList);
        logger.info("【page|行为】 最终格式 NetworkAuditUserLogDTO: {}", JSON.toJSONString(targetDTO));

        return targetDTO;
    }

}
