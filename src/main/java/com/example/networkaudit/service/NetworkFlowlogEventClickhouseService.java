package com.sama.networkaudit.service;

import com.core4ct.base.nokey.NkBaseService;
import com.sama.api.networkaudit.bean.*;
import com.sama.networkaudit.utils.data.bean.FlowLogBean;

import java.util.Date;
import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/14 13:43
 */
public interface NetworkFlowlogEventClickhouseService extends NkBaseService<NetworkFlowlogEventClickhouseDO> {

    /**
     * 批量插入
     *
     * @param list  List<NetworkFlowlogEventClickhouseDO>
     */
    void batchInsert(List<NetworkFlowlogEventClickhouseDO> list);

    /**
     * 批量（转换）迁移
     *
     * @param rawList   原始 FlowLogBean 列表
     */
    void batchMigrate(List<FlowLogBean> rawList);

    /**
     * 为 overview/flow 接口提供的特殊分页查询接口
     *
     * @param tenantOrgCode tenantOrgCode
     * @param inputParams   NetworkAuditSecurityLogDO
     * @param current       当前页
     * @param size          页大小
     * @param vmId          虚机id
     * @param poolId        poolId，可能为空
     * @return              List<NetworkAuditFlowLogV6DO>
     */
    List<NetworkAuditFlowLogV6DO> specifiedPage(String tenantOrgCode, NetworkAuditFlowLogV6DO inputParams, Integer current, Integer size, Long vmId, String poolId);

    /**
     * 为 /service/networkAudit 和 /overview/appLog/{range} 接口提供的特殊聚合查询接口
     *
     * @param tenantOrgCode 租户组织编码
     * @param logStartTime  日志开始时间
     * @param logEndTime    日志结束时间
     * @param vmId          虚机id
     * @param poolId        poolId，可能为空
     * @return              OverviewInfo
     */
    OverviewInfo overviewQuery(String tenantOrgCode, Date logStartTime, Date logEndTime, Long vmId, String poolId);

}
