package org.wesuper.jtools.hdscompare.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wesuper.jtools.hdscompare.starter.TableStructureCompareAutoStarter;
import org.wesuper.jtools.hdscompare.extractor.TableStructureExtractor;
import org.wesuper.jtools.hdscompare.extractor.TableStructureExtractorFactory;
import org.wesuper.jtools.hdscompare.extractor.MySqlTableStructureExtractor;
import org.wesuper.jtools.hdscompare.extractor.TidbTableStructureExtractor;
import java.util.List;

@Configuration
public class SchemaCompareAutoConfiguration {

    @Bean
    public TableStructureCompareAutoStarter tableStructureCompareAutoStarter() {
        return new TableStructureCompareAutoStarter();
    }

    @Bean
    public TableStructureExtractorFactory tableStructureExtractorFactory(List<TableStructureExtractor> extractors) {
        return new TableStructureExtractorFactory(extractors);
    }

    @Bean
    public MySqlTableStructureExtractor mySqlTableStructureExtractor() {
        return new MySqlTableStructureExtractor();
    }

    @Bean
    public TidbTableStructureExtractor tidbTableStructureExtractor() {
        return new TidbTableStructureExtractor();
    }

    // Add more beans as needed for schema comparison functionality
}