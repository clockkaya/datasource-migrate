package com.sama.networkaudit.dubboImpl;

import com.core4ct.base.nokey.impl.NkBaseDubboServiceImpl;
import com.sama.api.networkaudit.bean.NetworkFlowlogEventClickhouseDO;
import com.sama.api.networkaudit.service.NetworkFlowlogEventClickhouseDubboService;
import com.sama.networkaudit.service.NetworkFlowlogEventClickhouseService;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import jakarta.annotation.Resource;

/**
 * @author: huxh
 * @description: nothing to provide
 * @datetime: 2025/2/21 14:01
 */
@DubboService
@RefreshScope
public class NetworkFlowlogEventClickhouseDubboServiceImpl extends NkBaseDubboServiceImpl<NetworkFlowlogEventClickhouseDO,NetworkFlowlogEventClickhouseService> implements NetworkFlowlogEventClickhouseDubboService {

    @Resource
    private NetworkFlowlogEventClickhouseService networkFlowlogEventClickhouseService;
}
