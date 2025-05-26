// package org.wesuper.jtools.hdscompare;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.stereotype.Service;
// import org.wesuper.jtools.hdscompare.service.TableStructureCompareService;
// import org.wesuper.jtools.hdscompare.model.CompareResult;

// import java.util.List;

// @Service
// public class SchemaCompareExampleService {

//     @Autowired
//     private TableStructureCompareService compareService;

//     public void runComparison() {
//         // 执行所有配置的比对任务
//         List<CompareResult> results = compareService.compareAllConfiguredTables();
        
//         // 分析并打印结果
//         for (CompareResult result : results) {
//             System.out.println("比对结果: " + result.getSourceTable() + " vs " + result.getTargetTable());
//             if (result.isFullyMatched()) {
//                 System.out.println("表结构完全匹配");
//             } else {
//                 System.out.println("发现 " + result.getCriticalDifferenceCount() + " 个严重差异");
//                 System.out.println("发现 " + result.getWarningDifferenceCount() + " 个警告差异");
//                 System.out.println("发现 " + result.getNoticeDifferenceCount() + " 个通知差异");
//                 System.out.println("发现 " + result.getAcceptableDifferenceCount() + " 个可接受差异");
//             }
//             System.out.println("匹配度: " + result.getMatchPercentage() + "%");
//             System.out.println("----------------------------------------");
//         }
//     }
// } 