package com.sama.networkaudit.dubboImpl;

import com.alibaba.fastjson2.JSON;
import com.sama.api.networkaudit.bean.*;
import com.sama.api.networkaudit.service.NetworkAuditReportDubboService;
import com.sama.networkaudit.mapper.mysql.*;
import com.sama.networkaudit.service.*;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import jakarta.annotation.Resource;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.sama.networkaudit.constant.Constants.UNOFFICIAL_ENVS;

@DubboService
@RefreshScope
public class NetworkAuditReportDubboServiceImpl implements NetworkAuditReportDubboService {

    private final static Logger logger = LogManager.getLogger(NetworkAuditReportDubboService.class);

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Resource
    IpFlowStatsMapper ipFlowStatsMapper;

    @Resource
    AppFlowStatsMapper appFlowStatsMapper;

    @Resource
    WebBehaviorStatsMapper webBehaviorStatsMapper;

    @Resource
    WebSiteReqStatsMapper webSiteReqStatsMapper;

    @Resource
    IpsTypeStatsMapper ipsTypeStatsMapper;

    @Resource
    MaliciousStatsMapper maliciousStatsMapper;

    @Resource
    VirusStatsMapper virusStatsMapper;

    @Value("${init.env}")
    private String env;

    @Resource(name = "validDeprecatedFunc")
    ThreadPoolTaskExecutor validDeprecatedFunc;

    @Resource
    WebBehaviorStatsClickhouseService webBehaviorStatsClickhouseService;

    @Resource
    WebSiteReqStatsClickhouseService webSiteReqStatsClickhouseService;

    @Resource
    IpsTypeStatsClickhouseService ipsTypeStatsClickhouseService;

    @Resource
    MaliciousStatsClickhouseService maliciousStatsClickhouseService;

    @Resource
    VirusStatsClickhouseService virusStatsClickhouseService;

    @Resource
    AppFlowStatsClickhouseService appFlowStatsClickhouseService;

    @Resource
    IpFlowStatsClickhouseService ipFlowStatsClickhouseService;

    @Override
    public List<NetworkAuditOverviewFlowLogItem> getFlowOverview(String org_code_prefix, String start_time, String end_time){
        List<NetworkAuditOverviewFlowLogItem> ckRes = ipFlowStatsClickhouseService.getFlowOverview(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【ip_flow_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<NetworkAuditOverviewFlowLogItem> esRes = ipFlowStatsMapper.getFlowOverview(org_code_prefix,start_time,end_time);
                logger.info("【ip_flow_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }

    @Override
    public List<NetworkAuditOverviewAppLogItem> getAppFlowOverview(String org_code_prefix, String start_time, String end_time){
        List<NetworkAuditOverviewAppLogItem> ckRes = appFlowStatsClickhouseService.getAppFlowOverview(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【app_flow_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<NetworkAuditOverviewAppLogItem> esRes = appFlowStatsMapper.getAppFlowOverview(org_code_prefix,start_time,end_time);
                logger.info("【app_flow_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }
    @Override
    public List<WebBehaviorInfo> getWebBehaviorInfo(String org_code_prefix, String start_time, String end_time){
        List<WebBehaviorInfo> ckRes = webBehaviorStatsClickhouseService.getWebBehaviorInfo(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【web_behavior_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<WebBehaviorInfo> esRes = webBehaviorStatsMapper.getWebBehaviorInfo(org_code_prefix,start_time,end_time);
                logger.info("【web_behavior_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }
    @Override
    public List<AuditNormalInfo> getWebSiteReqInfo(String org_code_prefix, String start_time, String end_time){
        List<AuditNormalInfo> ckRes = webSiteReqStatsClickhouseService.getWebSiteReqInfo(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【web_site_req_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<AuditNormalInfo> esRes = webSiteReqStatsMapper.getWebSiteReqInfo(org_code_prefix,start_time,end_time);
                logger.info("【web_site_req_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }

    @Override
    public List<AuditNormalInfo> getIpsTypeStats(String org_code_prefix, String start_time, String end_time){
        List<AuditNormalInfo> ckRes = ipsTypeStatsClickhouseService.getIpsTypeStats(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【ips_type_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<AuditNormalInfo> esRes = ipsTypeStatsMapper.getIpsTypeStats(org_code_prefix,start_time,end_time);
                logger.info("【ips_type_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }

    @Override
    public List<TrendInfo> getIpsTypeTrend(String org_code_prefix, String start_time, String end_time){
        Date startDate, endDate;
        try {
            startDate = sdf.parse(start_time);
            endDate = sdf.parse(end_time);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        List<TrendInfo> timeItemSeries = Collections.emptyList();
        List<String> timeSeries;
        if (endDate.getTime() - startDate.getTime() > 1000 * 60 * 60 * 72) {
            // 超过72小时，按天计算
            timeItemSeries = ipsTypeStatsClickhouseService.getIpsTypeTrendByDay(org_code_prefix,start_time,end_time);
            timeSeries = generateTimeSeriesFromCalendar(startDate, endDate, 'd');
        } else {
            // 未超过72小时，按小时计算
            timeItemSeries = ipsTypeStatsClickhouseService.getIpsTypeTrendByHour(org_code_prefix,start_time,end_time);
            timeSeries = generateTimeSeriesFromCalendar(startDate, endDate, 'h');
        }

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【整合|IpsTypeTrend】 开始调用 es 旧方法验证 ---------->");
                List<TrendInfo> esRes;
                if (endDate.getTime() - startDate.getTime() > 1000 * 60 * 60 * 72) {
                    esRes = ipsTypeStatsMapper.getIpsTypeTrendByDay(org_code_prefix,start_time,end_time);
                } else {
                    esRes = ipsTypeStatsMapper.getIpsTypeTrendByHour(org_code_prefix,start_time,end_time);
                }
                logger.info("【整合|IpsTypeTrend】旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        HashMap<String, Long> timeMap = new HashMap<>();
        for (TrendInfo item : timeItemSeries) {
            timeMap.put(item.getInfo(), item.getCount());
        }
        ArrayList<TrendInfo> result = new ArrayList<>();
        assert timeSeries != null;
        for (String t : timeSeries) {
            TrendInfo trendInfo = new TrendInfo();
            trendInfo.setInfo(t);
            trendInfo.setCount(timeMap.getOrDefault(t, 0L));
            result.add(trendInfo);
        }
        return result;

    }

    @Override
    public List<AuditNormalInfo> getMaliciousStats(String org_code_prefix, String start_time, String end_time) {
        List<AuditNormalInfo> ckRes = maliciousStatsClickhouseService.getMaliciousStats(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【malicious_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<AuditNormalInfo> esRes = maliciousStatsMapper.getMaliciousStats(org_code_prefix,start_time,end_time);
                logger.info("【malicious_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }

    @Override
    public List<AuditNormalInfo> getVirusStats(String org_code_prefix, String start_time, String end_time) {
        List<AuditNormalInfo> ckRes = virusStatsClickhouseService.getVirusStats(org_code_prefix,start_time,end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【virus_stats|聚合】 开始调用 es 旧方法验证 ---------->");
                List<AuditNormalInfo> esRes = virusStatsMapper.getVirusStats(org_code_prefix,start_time,end_time);
                logger.info("【virus_stats|聚合】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        return ckRes;
    }

    /**
     * 整体重写
     */
    @Override
    public NetAuditSummaryInfo getNetAuditSummaryInfo(String org_code_prefix, String start_time, String end_time) {
        NetAuditSummaryInfo netAuditSummaryInfo = new NetAuditSummaryInfo();
        netAuditSummaryInfo.setIpsCnt(ipsTypeStatsClickhouseService.getIpsSum(org_code_prefix, start_time, end_time));
        netAuditSummaryInfo.setMaliciousCnt(maliciousStatsClickhouseService.getMaliciousSum(org_code_prefix,start_time,end_time));
        netAuditSummaryInfo.setAvCnt(virusStatsClickhouseService.getVirusSum(org_code_prefix,start_time,end_time));
        Map<String, Object> ckRes = webBehaviorStatsClickhouseService.getWebBehaviorSummary(org_code_prefix, start_time, end_time);
        // 特殊 webCnt、searchCnt 需要转key名
        netAuditSummaryInfo.setWebReqCnt((Long)ckRes.getOrDefault("webCnt", 0));
        netAuditSummaryInfo.setWebSearchCnt((Long)ckRes.getOrDefault("searchCnt", 0));
        netAuditSummaryInfo.setMailCnt((Long)ckRes.getOrDefault("mailCnt", 0));

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【整合|NetAudit】 开始调用 es 旧方法验证 ---------->");
                NetAuditSummaryInfo esRes = getNetAuditSummaryInfoES(org_code_prefix,start_time,end_time);
                logger.info("【整合|NetAudit】 旧方法验证结束 ----------> {}", JSON.toJSONString(esRes));
            }
        });

        logger.info("【整合|NetAudit】 最终格式 Map<String, Object>: {}", JSON.toJSONString(netAuditSummaryInfo));

        return netAuditSummaryInfo;
    }

    @Deprecated
    public NetAuditSummaryInfo getNetAuditSummaryInfoES(String org_code_prefix, String start_time, String end_time) {
        NetAuditSummaryInfo netAuditSummaryInfo = new NetAuditSummaryInfo();
        netAuditSummaryInfo.setIpsCnt(ipsTypeStatsMapper.getIpsSum(org_code_prefix,start_time,end_time)!= null ? ipsTypeStatsMapper.getIpsSum(org_code_prefix,start_time,end_time) : 0L);
        netAuditSummaryInfo.setMaliciousCnt(maliciousStatsMapper.getMaliciousSum(org_code_prefix,start_time,end_time)!= null ? maliciousStatsMapper.getMaliciousSum(org_code_prefix,start_time,end_time) : 0L);
        netAuditSummaryInfo.setAvCnt(virusStatsMapper.getVirusSum(org_code_prefix,start_time,end_time)!= null ? virusStatsMapper.getVirusSum(org_code_prefix,start_time,end_time) : 0L);
        Map<String, BigDecimal> webBehaviorSummary = webBehaviorStatsMapper.getWebBehaviorSummary(org_code_prefix, start_time, end_time);
        if (webBehaviorSummary == null) {
            webBehaviorSummary = new HashMap<>(); // mapper返回的map有可能为null，而list不会，只会为空list，所以这里需要对map做一个处理
        }
        netAuditSummaryInfo.setWebReqCnt(webBehaviorSummary.get("webReqCnt") != null ? webBehaviorSummary.get("webReqCnt").longValue() : 0L);
        netAuditSummaryInfo.setWebSearchCnt(webBehaviorSummary.get("webSearchCnt") != null ? webBehaviorSummary.get("webSearchCnt").longValue() : 0L);
        netAuditSummaryInfo.setMailCnt(webBehaviorSummary.get("mailCnt") != null ? webBehaviorSummary.get("mailCnt").longValue() : 0L);

        return netAuditSummaryInfo;
    }

    @Override
    public Long getWebReqSumFromWebSiteReqStats(String org_code_prefix, String start_time, String end_time) {
        Long ckRes = webSiteReqStatsClickhouseService.getWebSiteReqSum(org_code_prefix, start_time, end_time);

        validDeprecatedFunc.execute(() -> {
            if (UNOFFICIAL_ENVS.contains(env)){
                logger.info("【web_site_req_stats|全】 开始调用 es 旧方法验证 ---------->");
                Long esRes = webSiteReqStatsMapper.getWebSiteReqSum(org_code_prefix, start_time, end_time);
                logger.info("【web_site_req_stats|全】 旧方法验证结束 ----------> {}", esRes != null? esRes : 0L);
            }
        });

        // ck 方法保证了空为0，不用三元判断
        return ckRes;
    }


    /**
     * 按固定间隔生成时间点，注意endTime是包括在内的。
     *
     * @param startDate
     * @param endDate
     * @param dateFlag
     * @return
     * @throws ParseException
     */
    public static List<String> generateTimeSeriesFromCalendar(Date startDate, Date endDate, char dateFlag) {
        Calendar calendar = Calendar.getInstance();
//        Date date = sdf.parse(startTime);
        calendar.setTime(startDate);

        long endTime = endDate.getTime();

        LinkedList<String> list = new LinkedList<>();
        int field;
        switch (dateFlag) { // 注意char大小写没区别的
            case 'n':
                calendar.set(Calendar.SECOND, 0);
                field = Calendar.MINUTE;
                break;
            case 'h':
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MINUTE, 0);
                field = Calendar.HOUR_OF_DAY;
                break;
            case 'd':
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                field = Calendar.DATE;
                break;
            case 'w':
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                boolean isFirstSunday = (calendar.getFirstDayOfWeek() == Calendar.SUNDAY);
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
                dayOfWeek = isFirstSunday ? (dayOfWeek + 5) % 7 + 1 : dayOfWeek;
                calendar.add(Calendar.DATE, -dayOfWeek + 1);
                field = Calendar.WEEK_OF_MONTH;
                break;
            case 'm':
                calendar.set(Calendar.DATE, 1);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                field = Calendar.MONTH;
                break;
            case 'y':
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.DATE, 1);
                calendar.set(Calendar.MONTH, 0);
                field = Calendar.YEAR;
                break;
            default:
                return null;
        }
//        System.out.println(sdf.format(calendar.getTime()));
        while (calendar.getTimeInMillis() <= (endTime)) {
            list.add(sdf.format(calendar.getTime()));
            calendar.add(field, 1);
        }
        return list;
    }

}
