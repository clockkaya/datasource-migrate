package com.sama.networkaudit;


import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson2.JSON;
import com.core4ct.utils.DataUtils;
import com.sama.api.networkaudit.bean.*;
import com.sama.api.pool.object.DO.VMDO;
import com.sama.networkaudit.mapper.mysql.AssetInfoMapper;
import com.sama.networkaudit.utils.data.AssetInfoUtils;
import com.sama.networkaudit.utils.data.ESUtils;
import com.sama.networkaudit.utils.data.GetBeanUtils;
import com.sama.networkaudit.utils.data.bean.SecurityBean;
import jakarta.annotation.Resource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.sama.networkaudit.utils.data.indexEnum.*;

@SpringBootTest(classes = SamaNetworkAuditApplication.class)
public class NetworkAuditAbtLogTest {
    private static final Logger LOGGER = LogManager.getLogger(NetworkAuditAbtLogTest.class);




    @Resource
    GetBeanUtils getBean;

    @Resource
    AssetInfoMapper assetInfoMapper;

    @Resource
    AssetInfoUtils assetInfoUtils;

    @Resource
    ESUtils esUtils;






    @Test
    public void testAssetInfo(){
        VMDO vmdo = new VMDO();
        vmdo.setPoolId(87L);
        vmdo.setControlIp("192.168.11.177");
        //String string = assetInfoMapper.selectVmIpInfo(vmdo);
        //System.out.println("vmid"+string);
    }


    @Test
    public void testaggr(){
        String jsonWebBehavior = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"startTime\",\n" +
                "            \"lt\": \"endTime\"\n" +
                "          }\n" +

                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_asset_ip\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"asset_ip\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_asset_name\": {\n" +
                "                  \"terms\": {\n" +
                "                    \"field\": \"asset_name\",\n" +
                "                    \"size\": 9999,\n" +
                "                    \"order\": {\n" +
                "                      \"_count\": \"desc\"\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"aggs\": {\n" +
                "                    \"group_by_log_type\": {\n" +
                "                      \"terms\": {\n" +
                "                        \"field\": \"log_type\",\n" +
                "                        \"size\": 10,\n" +
                "                        \"order\": {\n" +
                "                          \"_count\": \"desc\"\n" +
                "                        }\n" +
                "                      },\n" +
                "                      \"aggs\": {\n" +
                "                        \"group_by_log_time\": {\n" +
                "                          \"date_histogram\": {\n" +
                "                            \"field\": \"log_time\",\n" +
                "                            \"interval\": \"hour\",\n" +
                "                            \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                          }\n" +
                "                        }\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";


        String newJson = jsonWebBehavior.replace("startTime", "2024-12-06 11:00:00").replace("endTime", "2024-12-06 11:30:00");
        List<Map<String, String>> aggrResults =  esUtils.aggQueryWebBehavior(newJson, "group_by_tenant_org_code", "group_by_asset_id", "group_by_asset_ip","group_by_asset_name","group_by_log_type","group_by_log_time", NETWORKUSERLOG.value());
        // 二次聚合 将多条一样的数据 elem.getTenant_org_code()+ "|" + elem.getAsset_id() +  "|"+ elem.getAsset_ip() + "|" + elem.getAsset_name() 聚合到一块
        // 通过 tenant_org_code+asset_id+asset_ip+assetname+logType 为唯一主键， 如果有不一样的
        Map<String, WebBehaviorStatsDO> webBehaviorMap = new HashMap<>();
        // asset_ip, asset_name, asset_id, tenant_org_code, event_time,       webcount, mailcount, searchcount, count

        for (Map<String, String> aggrResult: aggrResults) {
            LOGGER.info(aggrResult);
            try {
                if (DataUtils.isNotEmpty(aggrResult.get("group_by_log_type"))) {
                    String keyLogType = aggrResult.get("group_by_tenant_org_code") + "|" + aggrResult.get("group_by_asset_id") + "|" + aggrResult.get("group_by_asset_ip") + "|" + aggrResult.get("group_by_asset_name") + "|" + aggrResult.get("group_by_log_time");
                    if (webBehaviorMap.containsKey(keyLogType)) {
                        if (aggrResult.get("group_by_log_type").equalsIgnoreCase("web_access")) {
                            Long pre = webBehaviorMap.get(keyLogType).getWebCnt();
                            webBehaviorMap.get(keyLogType).setWebCnt(pre +Long.valueOf(aggrResult.get("count")));
                        } else if (aggrResult.get("group_by_log_type").equalsIgnoreCase("search_engine")) {
                            Long pre = webBehaviorMap.get(keyLogType).getSearchCnt();
                            webBehaviorMap.get(keyLogType).setSearchCnt(pre +Long.valueOf(aggrResult.get("count")));
                        } else if (aggrResult.get("group_by_log_type").equalsIgnoreCase("mail")) {
                            Long pre = webBehaviorMap.get(keyLogType).getMailCnt();
                            webBehaviorMap.get(keyLogType).setMailCnt(pre +Long.valueOf(aggrResult.get("count")));
                        }
                        Long totlapre = webBehaviorMap.get(keyLogType).getCount();
                        webBehaviorMap.get(keyLogType).setCount(totlapre + Long.valueOf(aggrResult.get("count")));
                    } else {
                        WebBehaviorStatsDO webBehaviorStatsDO = new WebBehaviorStatsDO();
                        webBehaviorStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        webBehaviorStatsDO.setAssetIp(aggrResult.get("group_by_asset_ip"));
                        webBehaviorStatsDO.setAssetName(aggrResult.get("group_by_asset_name"));
                        webBehaviorStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        webBehaviorStatsDO.seteventTime(date);
                        //
                        if (aggrResult.get("group_by_log_type").equalsIgnoreCase("web_access")) {
                            webBehaviorStatsDO.setWebCnt(Long.valueOf(aggrResult.get("count")));
                            webBehaviorStatsDO.setMailCnt(0L);
                            webBehaviorStatsDO.setSearchCnt(0L);
                            webBehaviorStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                        } else if (aggrResult.get("group_by_log_type").equalsIgnoreCase("search_engine")) {
                            webBehaviorStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                            webBehaviorStatsDO.setMailCnt(0L);
                            webBehaviorStatsDO.setWebCnt(0L);
                            webBehaviorStatsDO.setSearchCnt(Long.valueOf(aggrResult.get("count")));
                        } else if (aggrResult.get("group_by_log_type").equalsIgnoreCase("mail")) {
                            webBehaviorStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                            webBehaviorStatsDO.setMailCnt(Long.valueOf(aggrResult.get("count")));
                            webBehaviorStatsDO.setWebCnt(0L);
                            webBehaviorStatsDO.setSearchCnt(0L);
                        }
                        webBehaviorMap.put(keyLogType,webBehaviorStatsDO);
                    }
                } else {
                    System.out.println("竟然有数据的 log type 没有 " + aggrResult);
                }
            } catch (Exception e) {
                System.out.println("error occurred when giving values for webBehaviorStatsDO:");
                e.printStackTrace();
            }
        }

        LOGGER.info(webBehaviorMap);


        // 遍历webBehaviorMap

        List<WebBehaviorStatsDO> WebBehaviorStatsDOS = new ArrayList<>();
        for(Map.Entry<String, WebBehaviorStatsDO> key:webBehaviorMap.entrySet()){
            WebBehaviorStatsDO value = key.getValue();
            WebBehaviorStatsDOS.add(value);
        }

        LOGGER.info(WebBehaviorStatsDOS);

        LOGGER.info("22222222222222222222222222222222222");
        String jsonWebSite = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"log_type\": \"web_access\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"2024-12-06 11:00:00\",\n" +
                "            \"lt\": \"2024-12-06 11:30:00\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_url\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"url\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_log_time\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"log_time\",\n" +
                "                    \"interval\": \"hour\",\n" +
                "                    \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<WebSiteReqStatsDO> WebSiteReqStatsDOS = new ArrayList<>();
        try {
            List<Map<String, String>> aggrResults2 = esUtils.aggQuery(jsonWebSite, "group_by_tenant_org_code", "group_by_asset_id", "group_by_url", "group_by_log_time", NETWORKUSERLOG.value());
            for (Map<String, String> aggrResult: aggrResults2) {
                try {
                    if (DataUtils.isNotEmpty(aggrResult.get("group_by_url"))) {
                        WebSiteReqStatsDO webSiteReqStatsDO = new WebSiteReqStatsDO();
                        webSiteReqStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        webSiteReqStatsDO.setUrl(aggrResult.get("group_by_url"));
                        webSiteReqStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        webSiteReqStatsDO.seteventTime(date);
                        webSiteReqStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                        WebSiteReqStatsDOS.add(webSiteReqStatsDO);
                    }
                } catch (Exception e) {
                    LOGGER.error("error occurred when giving values for webSiteReqStatsDO:" );
                    e.printStackTrace();
                }
            }

        }catch (Exception e) {
            LOGGER.error("聚合 web site 失败");
            e.printStackTrace();
        }

        LOGGER.info(WebSiteReqStatsDOS);

        LOGGER.info("333333333333333333");

        LOGGER.info("ips statistics starts");

        String jsonIps = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"log_type\": \"ips\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"startTime\",\n" +
                "            \"lt\": \"endTime\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_event_type\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"event_type\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_log_time\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"ctime\",\n" +
                "                    \"interval\": \"hour\",\n" +
                "                    \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<IpsTypeStatsDO> IpsTypeStatsDOS = new ArrayList<>();
        try {
            String newjsonIps = jsonIps.replace("startTime", "2024-09-12 19:40:00").replace("endTime", "2024-09-12 20:20:00");
            List<Map<String, String>> aggrResults3 = esUtils.aggQuery(newjsonIps, "group_by_tenant_org_code", "group_by_asset_id", "group_by_event_type", "group_by_log_time", NETWORKSECURITYLOG.value());
            for (Map<String, String> aggrResult: aggrResults3) {
                try {
                    if (DataUtils.isNotEmpty(aggrResult.get("group_by_event_type"))) {
                        IpsTypeStatsDO ipsTypeStatsDO = new IpsTypeStatsDO();
                        ipsTypeStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        ipsTypeStatsDO.setEventType(aggrResult.get("group_by_event_type"));
                        ipsTypeStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        ipsTypeStatsDO.seteventTime(date);
                        ipsTypeStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                        IpsTypeStatsDOS.add(ipsTypeStatsDO);
                    }
                } catch (Exception e) {
                    LOGGER.error("error occurred when giving values for IpsTypeStatsDO:" );
                    e.printStackTrace();
                }
            }
        }catch (Exception e) {
            LOGGER.error("聚合 IPS 失败");
            e.printStackTrace();
        }

        LOGGER.info(IpsTypeStatsDOS);


        LOGGER.info("4444444444444444444444");

        LOGGER.info("malware_app statistics starts");

        String jsonMalwareApp = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"log_type\": \"malware_app\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"startTime\",\n" +
                "            \"lt\": \"endTime\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_web_name\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"web_name\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_log_time\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"ctime\",\n" +
                "                    \"interval\": \"hour\",\n" +
                "                    \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<MaliciousStatsDO> MaliciousStatsDOS = new ArrayList<>();

        try {
            String newjsonMalicious = jsonMalwareApp.replace("startTime", "2024-09-12 19:40:00").replace("endTime", "2024-09-12 20:20:00");
            List<Map<String, String>> aggrResults4 = esUtils.aggQuery(newjsonMalicious, "group_by_tenant_org_code", "group_by_asset_id", "group_by_web_name", "group_by_log_time", NETWORKSECURITYLOG.value());
            for (Map<String, String> aggrResult: aggrResults4) {
                try {
                    if (DataUtils.isNotEmpty(aggrResult.get("group_by_web_name"))) {
                        MaliciousStatsDO maliciousStatsDO = new MaliciousStatsDO();
                        maliciousStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        maliciousStatsDO.setWebName(aggrResult.get("group_by_web_name"));
                        maliciousStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        maliciousStatsDO.seteventTime(date);
                        maliciousStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                        MaliciousStatsDOS.add(maliciousStatsDO);
                    }
                } catch (Exception e) {
                    LOGGER.error("error occurred when giving values for MaliciousStatsDO:" );
                    e.printStackTrace();
                }
            }
        }catch (Exception e) {
            LOGGER.error("聚合 Malicious 失败");
            e.printStackTrace();
        }

        LOGGER.info(MaliciousStatsDOS);

        LOGGER.info("5555555555555555555555555555555555");

        LOGGER.info("virus statistics starts");
        String jsonVirus = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"log_type\": \"av\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"startTime\",\n" +
                "            \"lt\": \"endTime\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_virus_name\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"virus_name\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_log_time\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"ctime\",\n" +
                "                    \"interval\": \"hour\",\n" +
                "                    \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<VirusStatsDO> VirusStatsDOS = new ArrayList<>();

        try {
            String newjsonVirus = jsonVirus.replace("startTime", "2024-09-12 20:13:00").replace("endTime", "2024-09-12 20:14:00");
            List<Map<String, String>> aggrResults5 = esUtils.aggQuery(newjsonVirus, "group_by_tenant_org_code", "group_by_asset_id", "group_by_virus_name", "group_by_log_time", NETWORKSECURITYLOG.value());
            for (Map<String, String> aggrResult: aggrResults5) {
                try {
                    if (DataUtils.isNotEmpty(aggrResult.get("group_by_virus_name"))) {
                        VirusStatsDO virusStatsDO = new VirusStatsDO();
                        virusStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        virusStatsDO.setVirusName(aggrResult.get("group_by_virus_name"));
                        virusStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        virusStatsDO.seteventTime(date);
                        virusStatsDO.setCount(Long.valueOf(aggrResult.get("count")));
                        VirusStatsDOS.add(virusStatsDO);
                    }
                } catch (Exception e) {
                    LOGGER.error("error occurred when giving values for VirusStatsDO:" );
                    e.printStackTrace();
                }
            }
        }catch (Exception e) {
            LOGGER.error("聚合 virus 失败");
            e.printStackTrace();
        }

        LOGGER.info(VirusStatsDOS);


    }

    @Test
    public void testaggr2(){
        String appFlow = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"2024-09-13 14:30:00\",\n" +
                "            \"lt\": \"2024-09-13 14:50:00\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_appname\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"appname\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_log_time\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"log_time\",\n" +
                "                    \"interval\": \"hour\",\n" +
                "                    \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                  },\n" +
                "                  \"aggs\": {\n" +
                "                    \"total_bytes\": {\n" +
                "                      \"sum\": {\n" +
                "                        \"script\": {\n" +
                "                          \"source\": \"doc['up'].value + doc['down'].value\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<Map<String, String>> aggrResults5 = esUtils.aggQueryAppName(appFlow, "group_by_tenant_org_code", "group_by_asset_id", "group_by_appname", "group_by_log_time", "total_bytes",NETWORKFLOWLOG.value());
        LOGGER.info(aggrResults5);

        List<AppFlowStatsDO> AppFlowStatsDOS = new ArrayList<>();

        for (Map<String, String> aggrResult: aggrResults5) {
            try {
                if (DataUtils.isNotEmpty(aggrResult.get("group_by_appname"))) {
                    AppFlowStatsDO appFlowStatsDO = new AppFlowStatsDO();
                    appFlowStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                    appFlowStatsDO.setAppName(aggrResult.get("group_by_appname"));
                    appFlowStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                    SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = new Date();
                    try{
                        date = formatter2.parse(aggrResult.get("group_by_log_time"));
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                    appFlowStatsDO.seteventTime(date);
                    appFlowStatsDO.setTotalBytes(convertBytesFromStringToLong(aggrResult.get("total_bytes")));
                    AppFlowStatsDOS.add(appFlowStatsDO);
                }
            } catch (Exception e) {
                LOGGER.error("error occurred when giving values for VirusStatsDO:" );
                e.printStackTrace();
            }
        }

        LOGGER.info(AppFlowStatsDOS);



        String ipFlow = "{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"range\": {\n" +
                "          \"insert_es_time\": {\n" +
                "            \"gte\": \"2024-09-13 14:30:00\",\n" +
                "            \"lt\": \"2024-09-13 14:50:00\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_tenant_org_code\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tenant_org_code\",\n" +
                "        \"size\": 9999,\n" +
                "        \"order\": {\n" +
                "          \"_count\": \"desc\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"group_by_asset_id\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"asset_id\",\n" +
                "            \"size\": 9999,\n" +
                "            \"order\": {\n" +
                "              \"_count\": \"desc\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"group_by_asset_ip\": {\n" +
                "              \"terms\": {\n" +
                "                \"field\": \"asset_ip\",\n" +
                "                \"size\": 9999,\n" +
                "                \"order\": {\n" +
                "                  \"_count\": \"desc\"\n" +
                "                }\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"group_by_asset_name\": {\n" +
                "                  \"terms\": {\n" +
                "                    \"field\": \"asset_name\",\n" +
                "                    \"size\": 9999,\n" +
                "                    \"order\": {\n" +
                "                      \"_count\": \"desc\"\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"aggs\": {\n" +
                "                    \"group_by_log_time\": {\n" +
                "                      \"date_histogram\": {\n" +
                "                        \"field\": \"log_time\",\n" +
                "                        \"interval\": \"hour\",\n" +
                "                        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "                      },\n" +
                "                      \"aggs\": {\n" +
                "                        \"total_up\": {\n" +
                "                          \"sum\": {\n" +
                "                            \"field\": \"up\"\n" +
                "                          }\n" +
                "                        },\n" +
                "                        \"total_down\": {\n" +
                "                          \"sum\": {\n" +
                "                            \"field\": \"down\"\n" +
                "                          }\n" +
                "                        },\n" +
                "                        \"total_bytes\": {\n" +
                "                          \"bucket_script\": {\n" +
                "                            \"buckets_path\": {\n" +
                "                              \"up_sum\": \"total_up\",\n" +
                "                              \"down_sum\": \"total_down\"\n" +
                "                            },\n" +
                "                            \"script\": \"params.up_sum + params.down_sum\"\n" +
                "                          }\n" +
                "                        }\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";


        List<Map<String, String>> aggrResults =esUtils.aggQueryIpFlow(ipFlow, "group_by_tenant_org_code", "group_by_asset_id", "group_by_asset_ip","group_by_asset_name","group_by_log_time", "total_up", "total_down", "total_bytes",NETWORKFLOWLOG.value());
        LOGGER.info(aggrResults);

        List<IpFlowStatsDO> IpFlowStatsDOS = new ArrayList<>();

        try {
            List<Map<String, String>> aggrResults7 =esUtils.aggQueryIpFlow(ipFlow, "group_by_tenant_org_code", "group_by_asset_id", "group_by_asset_ip","group_by_asset_name","group_by_log_time", "total_up", "total_down", "total_bytes",NETWORKFLOWLOG.value());
            for (Map<String, String> aggrResult: aggrResults7) {
                try {
                    if (DataUtils.isNotEmpty(aggrResult.get("group_by_tenant_org_code"))) {
                        IpFlowStatsDO ipFlowStatsDO = new IpFlowStatsDO();
                        ipFlowStatsDO.setAssetId(Long.valueOf(aggrResult.get("group_by_asset_id")));
                        ipFlowStatsDO.setAssetIp(aggrResult.get("group_by_asset_ip"));
                        ipFlowStatsDO.setAssetName(aggrResult.get("group_by_asset_name"));
                        ipFlowStatsDO.setTenantOrgCode(aggrResult.get("group_by_tenant_org_code"));
                        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = new Date();
                        try{
                            date = formatter2.parse(aggrResult.get("group_by_log_time"));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        ipFlowStatsDO.seteventTime(date);
                        ipFlowStatsDO.setUpBytes(convertBytesFromStringToLong(aggrResult.get("total_up")));
                        ipFlowStatsDO.setDownBytes(convertBytesFromStringToLong(aggrResult.get("total_down")));
                        ipFlowStatsDO.setTotalBytes(convertBytesFromStringToLong(aggrResult.get("total_bytes")));
                        IpFlowStatsDOS.add(ipFlowStatsDO);
                    }
                } catch (Exception e) {
                    LOGGER.error("error occurred when giving values for ipFlowStatsDO:" );
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            LOGGER.error("聚合 ipFlowStatsDO 失败");
            e.printStackTrace();
        }


        LOGGER.info(IpFlowStatsDOS);




    }

    public Long convertBytesFromStringToLong(String value) {
        double doubleValue = Double.parseDouble(value);
        int intValue = (int) doubleValue;
        return Long.valueOf(intValue);
    }













    public String getDate(String value) {

        String s = value.split(";")[0].split(">")[1];

        int lastIndex = s.lastIndexOf(" ");

        String source = "";

        if (lastIndex == -1) {
            source = s;
        } else {
            source = s.substring(0, lastIndex);
        }
        try {
            SimpleDateFormat parserSDF = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH);
            Date date = parserSDF.parse(source);
            String dateTime = DateUtil.formatDateTime(date);
            String time = dateTime.substring(4);
            String year = Calendar.getInstance().get(Calendar.YEAR) + "";
            String log_date = year + time;

            return log_date;
        } catch (Exception e) {
            return "";
        }

    }
    @Test
    public void testhzh() throws InterruptedException {
        String value="192.168.10.172|<0>Aug 07 14:22:04 ABT;190112000728143117033369;ipv4;3; malware_app: user_name=192.168.10.188;user_group_name=anonymous;term_platform=未知类型;term_device=未知类型;src_ip=192.168.10.188;dst_ip=10.6.7.248;web_name=grupomodamil.com.br;url=http://grupomodamil.com.br/;msg=";
        String pool_org_code = "0124001100141007";
        String vmIp = "192.168.10.172";
        if (value.contains("ips:") || value.contains("malware_app:") || value.contains("av:")) {
            // IPS日志 恶意url日志 病毒日志
            SecurityBean securityBean = getBean.getSecurityBean(value);
            if (value.contains("malware_app:")){
                securityBean.setCtime(getDate(value));
            }
            securityBean.setPool_org_code(pool_org_code);
            securityBean.setVm_ip(vmIp);

            // 匹配虚机和资产信息
            securityBean.setVm_id(assetInfoUtils.getVmId(pool_org_code, vmIp));
            if (DataUtils.isEmpty(securityBean.getVm_id())) {
                LOGGER.debug("网审安全日志获取虚机id信息有误:" + value);

            }
            if (DataUtils.isEmpty(securityBean.getSrc_ip()) && DataUtils.isEmpty(securityBean.getDst_ip())){
                LOGGER.error("src_ip and dst_ip are both missing from : "+ value);
            } else if (DataUtils.isEmpty(securityBean.getSrc_ip()) && DataUtils.isNotEmpty(securityBean.getDst_ip())){
                String asset_id_dst = assetInfoUtils.getAssetId(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip());
                if (DataUtils.isEmpty(asset_id_dst)) {
                    LOGGER.error("网审安全日志获取资产ID信息有误 src_ip is missing, 源自dst_ip:" + value);
                } else {
                    securityBean.setAsset_ip(securityBean.getDst_ip());
                    Calendar calendar = Calendar.getInstance(); // get current instance of the calendar
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    securityBean.setInsert_es_time(formatter.format(calendar.getTime()));
                    securityBean.setAsset_id(asset_id_dst);
                    securityBean.setTenant_org_code(assetInfoUtils.getAssetOrgCode(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip()));
                    securityBean.setAsset_name(assetInfoUtils.getAssetName(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip()));
                    securityBean.setKafka_id(String.valueOf(UUID.randomUUID()));
                    String beanString = JSON.toJSONString(securityBean);
                    Map<String,String> dataMap=new HashMap<>();
                    dataMap.put("uuid",securityBean.getKafka_id());
                    dataMap.put("value",beanString);
                    // 406 推送小伙告警
                    try {
                        //ips 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("ips")) {
                            SecurityBean cloneIps = SerializationUtils.clone(securityBean);
                            cloneIps.setNotice_type(2);
                            cloneIps.setNotice_object(6);
                            cloneIps.setProduct_sub_table_num(1);
                            cloneIps.setEqu_manuf("042");
                            cloneIps.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneIps.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneIps.setDoc_dir_num("4.7.3.7.2");
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }
                        // av 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("av")) {
                            SecurityBean cloneAv = SerializationUtils.clone(securityBean);
                            cloneAv.setNotice_type(2);
                            cloneAv.setNotice_object(6);
                            cloneAv.setProduct_sub_table_num(1);
                            cloneAv.setEqu_manuf("042");
                            cloneAv.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneAv.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneAv.setDoc_dir_num("4.7.3.4");
                            cloneAv.setPriority(3);
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }

                    } catch (Exception e) {
                        LOGGER.error("告警推送异常！", e);
                    }
                }
            } else if (DataUtils.isNotEmpty(securityBean.getSrc_ip()) && DataUtils.isEmpty(securityBean.getDst_ip())) {
                String asset_id_src = assetInfoUtils.getAssetId(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip());
                if (DataUtils.isEmpty(asset_id_src)) {
                    LOGGER.error("网审安全日志获取资产ID信息有误 dst_ip is missing, 源自src_ip:" + value);
                } else {
                    securityBean.setAsset_ip(securityBean.getSrc_ip());
                    Calendar calendar = Calendar.getInstance(); // get current instance of the calendar
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    securityBean.setInsert_es_time(formatter.format(calendar.getTime()));
                    securityBean.setAsset_id(asset_id_src);
                    securityBean.setTenant_org_code(assetInfoUtils.getAssetOrgCode(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip()));
                    securityBean.setAsset_name(assetInfoUtils.getAssetName(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip()));
                    securityBean.setKafka_id("_" + String.valueOf(UUID.randomUUID()));
                    String beanString = JSON.toJSONString(securityBean);
                    Map<String,String> dataMap=new HashMap<>();
                    dataMap.put("uuid",securityBean.getKafka_id());
                    dataMap.put("value",beanString);
                    // 406 推送小伙告警
                    try {
                        //ips 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("ips")) {
                            SecurityBean cloneIps = SerializationUtils.clone(securityBean);
                            cloneIps.setNotice_type(2);
                            cloneIps.setNotice_object(6);
                            cloneIps.setProduct_sub_table_num(1);
                            cloneIps.setEqu_manuf("042");
                            cloneIps.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneIps.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneIps.setDoc_dir_num("4.7.3.7.2");
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }
                        // av 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("av")) {
                            SecurityBean cloneAv = SerializationUtils.clone(securityBean);
                            cloneAv.setNotice_type(2);
                            cloneAv.setNotice_object(6);
                            cloneAv.setProduct_sub_table_num(1);
                            cloneAv.setEqu_manuf("042");
                            cloneAv.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneAv.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneAv.setDoc_dir_num("4.7.3.4");
                            cloneAv.setPriority(3);
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }

                    } catch (Exception e) {
                        LOGGER.error("告警推送异常！", e);
                    }
                }
            } else {
                String asset_id_dst = assetInfoUtils.getAssetId(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip());
                if (DataUtils.isEmpty(asset_id_dst)) {
                    LOGGER.error("网审安全日志获取资产ID信息有误 源自dst_ip:" + value);
                } else {
                    securityBean.setAsset_ip(securityBean.getDst_ip());
                    Calendar calendar = Calendar.getInstance(); // get current instance of the calendar
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    securityBean.setInsert_es_time(formatter.format(calendar.getTime()));
                    securityBean.setAsset_id(asset_id_dst);
                    securityBean.setTenant_org_code(assetInfoUtils.getAssetOrgCode(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip()));
                    securityBean.setAsset_name(assetInfoUtils.getAssetName(pool_org_code, securityBean.getVm_id(), securityBean.getDst_ip()));
                    securityBean.setKafka_id( String.valueOf(UUID.randomUUID()));
                    String beanString = JSON.toJSONString(securityBean);
                    Map<String,String> dataMap=new HashMap<>();
                    dataMap.put("uuid",securityBean.getKafka_id());
                    dataMap.put("value",beanString);
                    LOGGER.info(beanString);
                    LOGGER.info("dst_ip is ok");
                    // 406 推送小伙告警
                    try {
                        //ips 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("ips")) {
                            SecurityBean cloneIps = SerializationUtils.clone(securityBean);
                            cloneIps.setNotice_type(2);
                            cloneIps.setNotice_object(6);
                            cloneIps.setProduct_sub_table_num(1);
                            cloneIps.setEqu_manuf("042");
                            cloneIps.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneIps.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneIps.setDoc_dir_num("4.7.3.7.2");
                            LOGGER.info(cloneIps);
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }
                        // av 日志推送中台
                        if (securityBean.getLog_type().equalsIgnoreCase("av")) {
                            SecurityBean cloneAv = SerializationUtils.clone(securityBean);
                            cloneAv.setNotice_type(2);
                            cloneAv.setNotice_object(6);
                            cloneAv.setProduct_sub_table_num(1);
                            cloneAv.setEqu_manuf("042");
                            cloneAv.setDev_type("002");
                            // pool_org_code + vm_id
                            cloneAv.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                            // pool_org_code
                            cloneAv.setDoc_dir_num("4.7.3.4");
                            cloneAv.setPriority(3);
                            LOGGER.info(cloneAv);
                            //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                            //sendNoticeInfo.sentNoticeMsg(beanString);
                        }

                    } catch (Exception e) {
                        LOGGER.error("告警推送异常！", e);
                    }
                }
                if ((!securityBean.getDst_ip().equalsIgnoreCase(securityBean.getSrc_ip()))){
                    LOGGER.info("src ip and dst ip are not the same");
                    LOGGER.info("securityBean is "+JSON.toJSONString(securityBean));
                    LOGGER.info("src ip and dst ip are not the same");
                    String asset_id_src = assetInfoUtils.getAssetId(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip());
                    if (DataUtils.isEmpty(asset_id_src)) {
                        LOGGER.error("网审安全日志获取资产ID信息有误 源自src_ip:" + value);
                    } else {
                        SecurityBean cloneIps = SerializationUtils.clone(securityBean);
                        LOGGER.info("securityBean is "+JSON.toJSONString(securityBean));
                        cloneIps.setAsset_ip(securityBean.getSrc_ip());
                        Calendar calendar = Calendar.getInstance(); // get current instance of the calendar
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        cloneIps.setInsert_es_time(formatter.format(calendar.getTime()));
                        cloneIps.setAsset_id(asset_id_src);
                        cloneIps.setTenant_org_code(assetInfoUtils.getAssetOrgCode(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip()));
                        cloneIps.setAsset_name(assetInfoUtils.getAssetName(pool_org_code, securityBean.getVm_id(), securityBean.getSrc_ip()));
                        cloneIps.setKafka_id(String.valueOf(UUID.randomUUID()));
                        String beanString = JSON.toJSONString(cloneIps);
                        Map<String,String> dataMap=new HashMap<>();
                        dataMap.put("uuid",cloneIps.getKafka_id());
                        dataMap.put("value",beanString);
                        LOGGER.info("src ip and dst ip  are ok ");
                        // 406 推送小伙告警
                        try {
                            //ips 日志推送中台
                            if (cloneIps.getLog_type().equalsIgnoreCase("malware_app")) {
                                SecurityBean cloneIpsAll = SerializationUtils.clone(cloneIps);
                                cloneIpsAll.setNotice_type(2);
                                cloneIpsAll.setNotice_object(6);
                                cloneIpsAll.setProduct_sub_table_num(1);
                                cloneIpsAll.setEqu_manuf("042");
                                cloneIpsAll.setDev_type("002");
                                // pool_org_code + vm_id
                                cloneIpsAll.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                                // pool_org_code
                                cloneIpsAll.setDoc_dir_num("4.7.3.7.2");
                                LOGGER.info("cloneIpsAll "+JSON.toJSONString(cloneIpsAll));
                                LOGGER.info("cloneIps "+JSON.toJSONString(cloneIps));
                                //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                                //sendNoticeInfo.sentNoticeMsg(beanString);
                            }
                            // av 日志推送中台
                            if (cloneIps.getLog_type().equalsIgnoreCase("av")) {
                                SecurityBean cloneAv = SerializationUtils.clone(cloneIps);
                                cloneAv.setNotice_type(2);
                                cloneAv.setNotice_object(6);
                                cloneAv.setProduct_sub_table_num(1);
                                cloneAv.setEqu_manuf("042");
                                cloneAv.setDev_type("002");
                                // pool_org_code + vm_id
                                cloneAv.setDevice_id(pool_org_code+"_"+securityBean.getVm_id());
                                // pool_org_code
                                cloneAv.setDoc_dir_num("4.7.3.4");
                                cloneAv.setPriority(3);
                                LOGGER.info(cloneAv);
                                //NoticeDatabaseAuditDO noticeInfoDO = convertNoticeInfo(bean.dst_ipv4, AbilityInfoEnum.DATABASEAUDIT.getAbilityId(), Long.valueOf(poolId), bean.asset_id, bean, record.value());
                                //sendNoticeInfo.sentNoticeMsg(beanString);
                            }

                        } catch (Exception e) {
                            LOGGER.error("告警推送异常！", e);
                        }
                    }
                }
            }
        }
    }




}
