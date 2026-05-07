package org.hestiastore.index.segmentindex;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Enforces SegmentIndex package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String SEGMENT_INDEX_PACKAGES = "org.hestiastore.index.segmentindex..";
    private static final String SEGMENT_REGISTRY_PACKAGES = "org.hestiastore.index.segmentregistry..";
    private static final String SEGMENT_INDEX_CORE_STORAGE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.storage..";
    private static final String SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES =
            "org.hestiastore.index.segmentindex.core.topology..";
    private static final String SEGMENT_INDEX_CORE_SESSION_PACKAGES =
            "org.hestiastore.index.segmentindex.core.session..";
    private static final String REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES =
            "org.hestiastore.index.segmentindex.config..";
    private static final String REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES =
            "org.hestiastore.index.segmentindex.runtimeconfiguration..";
    private static final String REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES =
            "org.hestiastore.index.segmentindex.core.control..";
    private static final String SEGMENT_INDEX_PUBLIC_API_PACKAGE =
            "org.hestiastore.index.segmentindex";

    @ArchTest
    static final ArchRule only_segment_index_packages_depend_on_segment_registry = noClasses()//
            .that() //
            .resideOutsideOfPackages(SEGMENT_INDEX_PACKAGES,
                    SEGMENT_REGISTRY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_REGISTRY_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_storage_does_not_depend_on_session = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SESSION_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_topology_does_not_depend_on_session = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SESSION_PACKAGES);

    @ArchTest
    static final ArchRule removed_segment_index_packages_are_not_used_by_internal_code = noClasses()//
            .that()//
            .resideOutsideOfPackages(REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES,
                    SEGMENT_INDEX_PUBLIC_API_PACKAGE)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES);
}
