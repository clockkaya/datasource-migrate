package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.IpFlowStatsClickhouseDO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2025/4/3 14:10
 */
@DS("clickhouse")
public interface IpFlowStatsClickhouseMapper extends NkBaseMapper<IpFlowStatsClickhouseDO> {

    List<IpFlowStatsClickhouseDO> plainSelect();

    List<IpFlowStatsClickhouseDO> customSelect(@Param("cm") IpFlowStatsClickhouseDO queryDO);

}
