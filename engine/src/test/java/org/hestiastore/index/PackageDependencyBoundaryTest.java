package org.hestiastore.index;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.dependencies.SliceAssignment;
import com.tngtech.archunit.library.dependencies.SliceIdentifier;

/**
 * Enforces package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String INDEX_PACKAGES = "org.hestiastore.index..";
    private static final String INDEX_PACKAGE_PREFIX = "org.hestiastore.index.";
    private static final String SEGMENT_INDEX_FACADE_PACKAGE = "org.hestiastore.index.segmentindex";
    private static final String SEGMENT_PACKAGES = "org.hestiastore.index.segment..";
    private static final String SORTED_DATA_FILE_PACKAGE = "org.hestiastore.index.sorteddatafile";
    private static final String CACHE_PACKAGES = "org.hestiastore.index.cache..";
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
    static final ArchRule index_packages_are_free_of_cycles = slices()
            .assignedFrom(new IndexPackageSliceAssignment())
            .namingSlices("Slice $1")
            .should().beFreeOfCycles();

    @ArchTest
    static final ArchRule core_packages_do_not_depend_on_monitoring_management_or_console = noClasses().that()
            .resideInAPackage(INDEX_PACKAGES)
            .and().resideOutsideOfPackages(CORE_EXCLUDED_PACKAGES)
            .should().dependOnClassesThat()
            .resideInAnyPackage(CORE_FORBIDDEN_PACKAGES);

    @ArchTest
    static final ArchRule segment_package_does_not_depend_on_segment_index_packages = noClasses().that()
            .resideInAPackage(SEGMENT_PACKAGES)
            .should().dependOnClassesThat()
            .resideInAnyPackage(SEGMENT_FORBIDDEN_PACKAGES);

    @ArchTest
    static final ArchRule sorted_data_file_primitives_do_not_depend_on_cache = noClasses().that()
            .resideInAPackage(SORTED_DATA_FILE_PACKAGE)
            .should().dependOnClassesThat()
            .resideInAnyPackage(CACHE_PACKAGES);

    private static final class IndexPackageSliceAssignment
            implements SliceAssignment {

        @Override
        public SliceIdentifier getIdentifierOf(final JavaClass javaClass) {
            final String packageName = javaClass.getPackageName();
            if (SEGMENT_INDEX_FACADE_PACKAGE.equals(packageName)
                    || !packageName.startsWith(INDEX_PACKAGE_PREFIX)) {
                return SliceIdentifier.ignore();
            }
            return SliceIdentifier.of(
                    packageName.substring(INDEX_PACKAGE_PREFIX.length()));
        }

        @Override
        public String getDescription() {
            return "index packages excluding the segmentindex facade";
        }
    }
}
