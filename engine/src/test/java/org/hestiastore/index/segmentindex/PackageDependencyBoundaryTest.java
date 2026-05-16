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
    private static final String SEGMENT_INDEX_CORE_SEGMENT_LEASE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.segmentlease..";
    private static final String SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES =
            "org.hestiastore.index.segmentindex.core.bootstrap..";
    private static final String SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES =
            "org.hestiastore.index.segmentindex.core.operations..";
    private static final String SEGMENT_INDEX_CORE_MAINTENANCE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.maintenance..";
    private static final String SEGMENT_INDEX_CORE_SPLIT_PACKAGES =
            "org.hestiastore.index.segmentindex.core.split..";
    private static final String SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES =
            "org.hestiastore.index.segmentindex.core.executorregistry..";
    private static final String SEGMENT_INDEX_METRICS_PACKAGES =
            "org.hestiastore.index.segmentindex.metrics..";
    private static final String SEGMENT_INDEX_MAPPING_PACKAGES =
            "org.hestiastore.index.segmentindex.mapping..";
    private static final String SEGMENT_INDEX_EFFECTIVE_CONFIGURATION_PACKAGES =
            "org.hestiastore.index.segmentindex.configuration.effective..";
    private static final String SEGMENT_INDEX_TUNING_CONFIGURATION_PACKAGES =
            "org.hestiastore.index.segmentindex.configuration.tuning..";
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
    static final ArchRule segment_index_topology_does_not_depend_on_segment_lease = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SEGMENT_LEASE_PACKAGES);

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

    @ArchTest
    static final ArchRule effective_configuration_does_not_depend_on_runtime_tuning = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_EFFECTIVE_CONFIGURATION_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_TUNING_CONFIGURATION_PACKAGES);

    @ArchTest
    static final ArchRule bootstrap_does_not_depend_on_segment_index_public_api_package = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAPackage(SEGMENT_INDEX_PUBLIC_API_PACKAGE);

    @ArchTest
    static final ArchRule mapping_does_not_depend_on_split = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_MAPPING_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES);

    @ArchTest
    static final ArchRule operational_runtime_packages_do_not_depend_on_metrics = noClasses()//
            .that()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES,
                    SEGMENT_INDEX_CORE_MAINTENANCE_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_METRICS_PACKAGES);
}
