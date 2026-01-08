package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.WebBehaviorStatsClickhouseDO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2025/4/3 14:10
 */
@DS("clickhouse")
public interface WebBehaviorStatsClickhouseMapper extends NkBaseMapper<WebBehaviorStatsClickhouseDO> {

    List<WebBehaviorStatsClickhouseDO> plainSelect();

    /**
     * 注意不要擅自加：
     *             <otherwise>
     *                 tenant_org_code,
     *                 asset_id,
     *                 event_time,
     *             </otherwise>
     *
     * @param queryDO
     * @return
     */
    List<WebBehaviorStatsClickhouseDO> customSelect(@Param("cm") WebBehaviorStatsClickhouseDO queryDO);

}
