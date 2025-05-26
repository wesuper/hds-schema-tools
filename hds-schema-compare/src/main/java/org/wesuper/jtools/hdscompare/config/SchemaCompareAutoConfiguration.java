package org.wesuper.jtools.hdscompare.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wesuper.jtools.hdscompare.starter.TableStructureCompareAutoStarter;

@Configuration
public class SchemaCompareAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "jtools.hdscompare.auto-starter", name = "enabled", havingValue = "true", matchIfMissing = true)
    public TableStructureCompareAutoStarter tableStructureCompareAutoStarter() {
        return new TableStructureCompareAutoStarter();
    }

    // Add more beans as needed for schema comparison functionality
}