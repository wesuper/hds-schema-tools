package org.immortal.hydra.hdstbcomp.starter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.ApplicationArguments;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.model.CompareResult;
import org.wesuper.jtools.hdscompare.service.TableStructureCompareService;
import org.wesuper.jtools.hdscompare.starter.TableStructureCompareAutoStarter;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

class TableStructureCompareAutoStarterTest {

    @Mock
    private DataSourceCompareConfig dataSourceConfig;

    @Mock
    private TableStructureCompareService compareService;

    @Mock
    private ApplicationArguments applicationArguments;

    @InjectMocks
    private TableStructureCompareAutoStarter autoStarter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testRunWhenAutoCompareDisabled() throws Exception {
        // 设置自动比对为禁用
        when(dataSourceConfig.isAutoCompareOnStartup()).thenReturn(false);

        // 执行测试
        autoStarter.run(applicationArguments);

        // 验证compareService没有被调用
        verify(compareService, never()).compareAllConfiguredTables();
    }

    @Test
    void testRunWhenAutoCompareEnabled() throws Exception {
        // 设置自动比对为启用
        when(dataSourceConfig.isAutoCompareOnStartup()).thenReturn(true);
        when(dataSourceConfig.isVerboseOutput()).thenReturn(true);

        // 模拟比对结果
        List<CompareResult> results = new ArrayList<>();
        CompareResult result = new CompareResult("test_table");
        result.setFullyMatched(true);
        results.add(result);

        when(compareService.compareAllConfiguredTables()).thenReturn(results);

        // 执行测试
        autoStarter.run(applicationArguments);

        // 验证compareService被调用
        verify(compareService, times(1)).compareAllConfiguredTables();
    }

    @Test
    void testRunWithDifferences() throws Exception {
        // 设置自动比对为启用
        when(dataSourceConfig.isAutoCompareOnStartup()).thenReturn(true);
        when(dataSourceConfig.isVerboseOutput()).thenReturn(true);

        // 创建包含差异的比对结果
        List<CompareResult> results = new ArrayList<>();
        CompareResult result = new CompareResult("test_table");
        result.setFullyMatched(false);
        result.setMatchPercentage(80.0);
        
        // 添加列差异
        CompareResult.ColumnDifference columnDiff = new CompareResult.ColumnDifference(
                CompareResult.DifferenceType.COLUMN_MISSING,
                CompareResult.DifferenceLevel.WARNING,
                "test_column差异",
                "test_column"
        );
        result.getColumnDifferences().add(columnDiff);
        
        // 添加索引差异
        CompareResult.IndexDifference indexDiff = new CompareResult.IndexDifference(
                CompareResult.DifferenceType.INDEX_MISSING,
                CompareResult.DifferenceLevel.CRITICAL,
                "test_index差异",
                "test_index"
        );
        result.getIndexDifferences().add(indexDiff);
        
        results.add(result);

        when(compareService.compareAllConfiguredTables()).thenReturn(results);

        // 执行测试
        autoStarter.run(applicationArguments);

        // 验证compareService被调用
        verify(compareService, times(1)).compareAllConfiguredTables();
    }

    @Test
    void testRunWithEmptyResults() throws Exception {
        // 设置自动比对为启用
        when(dataSourceConfig.isAutoCompareOnStartup()).thenReturn(true);

        // 模拟空结果
        when(compareService.compareAllConfiguredTables()).thenReturn(new ArrayList<>());

        // 执行测试
        autoStarter.run(applicationArguments);

        // 验证compareService被调用
        verify(compareService, times(1)).compareAllConfiguredTables();
    }

    @Test
    void testRunWithException() throws Exception {
        // 设置自动比对为启用
        when(dataSourceConfig.isAutoCompareOnStartup()).thenReturn(true);

        // 模拟异常
        when(compareService.compareAllConfiguredTables()).thenThrow(new RuntimeException("Test exception"));

        // 执行测试
        autoStarter.run(applicationArguments);

        // 验证compareService被调用
        verify(compareService, times(1)).compareAllConfiguredTables();
    }
} 