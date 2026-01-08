package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.NeworkSecuritylogEventClickhouseDO;
import com.sama.api.networkaudit.bean.SecurityEventInfo;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/11 14:19
 */
@DS("clickhouse")
public interface NetworkSecuritylogEventClickhouseMapper extends NkBaseMapper<NeworkSecuritylogEventClickhouseDO> {

    List<NeworkSecuritylogEventClickhouseDO> selectAll();

    void batchInsert(@Param("list") List<NeworkSecuritylogEventClickhouseDO> list);

    void truncateTable();

    List<NeworkSecuritylogEventClickhouseDO> pageByCondAndOrd(@Param("cm") NeworkSecuritylogEventClickhouseDO queryDO, Page<NeworkSecuritylogEventClickhouseDO> page);

    List<SecurityEventInfo> listGroupByLogType(@Param("cm") NeworkSecuritylogEventClickhouseDO queryDO);

}
