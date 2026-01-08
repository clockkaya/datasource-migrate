package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.NetworkAuditOverviewAppLogItem;
import com.sama.api.networkaudit.bean.NetworkAuditOverviewFlowLogItem;
import com.sama.api.networkaudit.bean.NetworkFlowlogEventClickhouseDO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/11 14:19
 */
@DS("clickhouse")
public interface NetworkFlowlogEventClickhouseMapper extends NkBaseMapper<NetworkFlowlogEventClickhouseDO> {

    List<NetworkFlowlogEventClickhouseDO> selectAll();

    void batchInsert(@Param("list") List<NetworkFlowlogEventClickhouseDO> list);

    void truncateTable();

    List<NetworkFlowlogEventClickhouseDO> pageByCondAndOrd(@Param("cm") NetworkFlowlogEventClickhouseDO queryDO, Page<NetworkFlowlogEventClickhouseDO> page);

    List<NetworkFlowlogEventClickhouseDO> listFlowTrafficGroupByTime(@Param("cm") NetworkFlowlogEventClickhouseDO queryDO);

    List<NetworkAuditOverviewAppLogItem> listGroupByAppname(@Param("cm") NetworkFlowlogEventClickhouseDO queryDO);

    List<NetworkAuditOverviewFlowLogItem> listGroupByUip(@Param("cm") NetworkFlowlogEventClickhouseDO queryDO);

}
