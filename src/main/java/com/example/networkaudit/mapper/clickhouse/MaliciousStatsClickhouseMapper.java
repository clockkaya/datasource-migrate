package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.MaliciousStatsClickhouseDO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2025/4/3 14:10
 */
@DS("clickhouse")
public interface MaliciousStatsClickhouseMapper extends NkBaseMapper<MaliciousStatsClickhouseDO> {

    List<MaliciousStatsClickhouseDO> plainSelect();

    List<MaliciousStatsClickhouseDO> customSelect(@Param("cm") MaliciousStatsClickhouseDO queryDO);

}
