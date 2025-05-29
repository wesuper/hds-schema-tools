package org.wesuper.jtools.hdscompare.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wesuper.jtools.hdscompare.extractor.ElasticsearchTableStructureExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch客户端配置类
 * 用于创建和管理Elasticsearch RestHighLevelClient
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
@ConditionalOnProperty(prefix = "jtools.hdscompare.elasticsearch", name = "enabled", havingValue = "true", matchIfMissing = false)
public class ElasticsearchClientConfig {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClientConfig.class);

    /**
     * Elasticsearch配置属性
     */
    @Bean
    @ConfigurationProperties(prefix = "jtools.hdscompare.elasticsearch.props")
    public ElasticsearchProperties elasticsearchProperties() {
        return new ElasticsearchProperties();
    }

    /**
     * 创建Elasticsearch客户端映射
     *
     * @param properties Elasticsearch配置属性
     * @return Elasticsearch客户端映射
     */
    @Bean
    public Map<String, RestHighLevelClient> elasticsearchClientMap(@Qualifier("elasticsearchProperties") ElasticsearchProperties properties) {
        Map<String, RestHighLevelClient> clientMap = new HashMap<>();
        
        if (properties.getClients() != null) {
            properties.getClients().forEach((name, config) -> {
                try {
                    RestHighLevelClient client = createClient(config);
                    clientMap.put(name, client);
                    logger.info("Created Elasticsearch client: {} with hosts: {}", name, config.getHosts());
                } catch (Exception e) {
                    logger.error("Failed to create Elasticsearch client {}", name, e);
                }
            });
        }
        
        return clientMap;
    }
    
    /**
     * 创建单个Elasticsearch客户端
     *
     * @param config 客户端配置
     * @return RestHighLevelClient
     */
    private RestHighLevelClient createClient(ElasticsearchProperties.ClientConfig config) {
        HttpHost[] httpHosts = config.getHosts().stream()
                .map(host -> HttpHost.create(host))
                .toArray(HttpHost[]::new);
        
        return new RestHighLevelClient(
                RestClient.builder(httpHosts)
                        .setRequestConfigCallback(requestConfigBuilder -> 
                                requestConfigBuilder
                                        .setConnectTimeout(config.getConnectTimeout())
                                        .setSocketTimeout(config.getSocketTimeout())
                        )
        );
    }
    
    @Bean
    public ElasticsearchTableStructureExtractor elasticsearchTableStructureExtractor() {
        return new ElasticsearchTableStructureExtractor(elasticsearchClientMap(elasticsearchProperties()));
    }

    /**
     * Elasticsearch配置属性类
     */
    public static class ElasticsearchProperties {
        /**
         * 是否启用Elasticsearch
         */
        private boolean enabled = false;
        
        /**
         * Elasticsearch客户端配置映射
         */
        private Map<String, ClientConfig> clients = new HashMap<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Map<String, ClientConfig> getClients() {
            return clients;
        }

        public void setClients(Map<String, ClientConfig> clients) {
            this.clients = clients;
        }
        
        /**
         * 单个Elasticsearch客户端配置
         */
        public static class ClientConfig {
            /**
             * Elasticsearch主机列表
             */
            private List<String> hosts = new ArrayList<>();
            
            /**
             * 连接超时时间（毫秒）
             */
            private int connectTimeout = 5000;
            
            /**
             * 套接字超时时间（毫秒）
             */
            private int socketTimeout = 60000;

            public List<String> getHosts() {
                return hosts;
            }

            public void setHosts(List<String> hosts) {
                this.hosts = hosts;
            }

            public int getConnectTimeout() {
                return connectTimeout;
            }

            public void setConnectTimeout(int connectTimeout) {
                this.connectTimeout = connectTimeout;
            }

            public int getSocketTimeout() {
                return socketTimeout;
            }

            public void setSocketTimeout(int socketTimeout) {
                this.socketTimeout = socketTimeout;
            }
        }
    }
} 