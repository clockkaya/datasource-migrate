package com.sama.networkaudit.dubboImpl;

import com.core4ct.base.nokey.impl.NkBaseDubboServiceImpl;
import com.sama.api.networkaudit.bean.NetworkUserlogEventClickhouseDO;
import com.sama.api.networkaudit.service.NetworkUserlogEventClickhouseDubboService;
import com.sama.networkaudit.service.NetworkUserlogEventClickhouseService;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import jakarta.annotation.Resource;

/**
 * @author: huxh
 * @description: nothing to provide
 * @datetime: 2025/2/21 14:02
 */
@DubboService
@RefreshScope
public class NetworkUserlogEventClickhouseDubboServiceImpl extends NkBaseDubboServiceImpl<NetworkUserlogEventClickhouseDO,NetworkUserlogEventClickhouseService> implements NetworkUserlogEventClickhouseDubboService {

    @Resource
    private NetworkUserlogEventClickhouseService networkUserlogEventClickhouseService;
}
