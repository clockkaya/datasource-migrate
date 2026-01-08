package com.sama.networkaudit.mapper.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.core4ct.base.nokey.NkBaseMapper;
import com.sama.api.networkaudit.bean.NetworkUserlogEventClickhouseDO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author: huxh
 * @description:
 * @datetime: 2024/10/11 14:19
 */
@DS("clickhouse")
public interface NetworkUserlogEventClickhouseMapper extends NkBaseMapper<NetworkUserlogEventClickhouseDO> {

    List<NetworkUserlogEventClickhouseDO> selectAll();

    void batchInsert(@Param("list") List<NetworkUserlogEventClickhouseDO> list);

    void truncateTable();

    List<NetworkUserlogEventClickhouseDO> pageByCondAndOrd(@Param("cm") NetworkUserlogEventClickhouseDO queryDO, Page<NetworkUserlogEventClickhouseDO> page);

}
