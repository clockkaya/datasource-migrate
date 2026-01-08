package com.sama.networkaudit.service;

import com.core4ct.base.nokey.NkBaseService;
import com.sama.api.networkaudit.bean.NetworkAuditUserLogDO;
import com.sama.api.networkaudit.bean.NetworkAuditUserLogDTO;
import com.sama.api.networkaudit.bean.NetworkUserlogEventClickhouseDO;
import com.sama.networkaudit.utils.data.bean.UserBean;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/14 13:43
 */
public interface NetworkUserlogEventClickhouseService extends NkBaseService<NetworkUserlogEventClickhouseDO> {

    /**
     * 批量插入
     *
     * @param list  List<NetworkUserlogEventClickhouseDO>
     */
    void batchInsert(List<NetworkUserlogEventClickhouseDO> list);

    /**
     * 批量（转换）迁移
     *
     * @param rawList   原始 UserBean 列表
     */
    void batchMigrate(List<UserBean> rawList);

    /**
     * 为 /networkAudit/log/pageV6 接口提供的特殊分页查询接口
     *
     * @param tenantOrgCode tenantOrgCode
     * @param inputParams   NetworkAuditUserLogDO
     * @param current       当前页
     * @param size          页大小
     * @return              NetworkAuditUserLogDTO
     */
    NetworkAuditUserLogDTO specifiedPage(String tenantOrgCode, NetworkAuditUserLogDO inputParams, Integer current, Integer size);

}
