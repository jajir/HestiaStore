package org.hestiastore.index.segmentindex.core.bootstrap;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Enforces bootstrap package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index",
        importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String SEGMENT_INDEX_PUBLIC_API_PACKAGE =
            "org.hestiastore.index.segmentindex";
    private static final String SEGMENT_INDEX_BOOTSTRAP_PACKAGES =
            "org.hestiastore.index.segmentindex.core.bootstrap..";

    @ArchTest
    static final ArchRule only_segment_index_public_api_depends_on_bootstrap =
            noClasses().that().resideOutsideOfPackages(
                    SEGMENT_INDEX_PUBLIC_API_PACKAGE,
                    SEGMENT_INDEX_BOOTSTRAP_PACKAGES)
                    .should().dependOnClassesThat()
                    .resideInAnyPackage(SEGMENT_INDEX_BOOTSTRAP_PACKAGES);
}
