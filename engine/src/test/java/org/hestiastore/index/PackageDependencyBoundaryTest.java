package org.hestiastore.index;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Enforces package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index",
        importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String INDEX_PACKAGES = "org.hestiastore.index..";
    private static final String SEGMENT_PACKAGES =
            "org.hestiastore.index.segment..";
    private static final String[] CORE_FORBIDDEN_PACKAGES = {
            "org.hestiastore.index.monitoring..",
            "org.hestiastore.index.management..",
            "org.hestiastore.monitoring..",
            "org.hestiastore.management..",
            "org.hestiastore.console.." };
    private static final String[] CORE_EXCLUDED_PACKAGES = {
            "org.hestiastore.index.monitoring..",
            "org.hestiastore.index.management..",
            "org.hestiastore.console.." };
    private static final String[] SEGMENT_FORBIDDEN_PACKAGES = {
            "org.hestiastore.index.segmentindex.." };

    @ArchTest
    static final ArchRule core_packages_do_not_depend_on_monitoring_management_or_console =
            noClasses().that().resideInAPackage(INDEX_PACKAGES)
                    .and().resideOutsideOfPackages(CORE_EXCLUDED_PACKAGES)
                    .should().dependOnClassesThat()
                    .resideInAnyPackage(CORE_FORBIDDEN_PACKAGES);

    @ArchTest
    static final ArchRule segment_package_does_not_depend_on_segment_index_packages =
            noClasses().that().resideInAPackage(SEGMENT_PACKAGES)
                    .should().dependOnClassesThat()
                    .resideInAnyPackage(SEGMENT_FORBIDDEN_PACKAGES);
}
