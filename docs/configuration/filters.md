---
title: Filter Configuration
audience: user
doc_type: reference
owner: engine
---

# Filter Configuration

This page covers how to configure chunk filter pipelines on
`IndexConfiguration`.

For filter behavior, ordering rationale, and integrity semantics, see
[Filters & Integrity](../architecture/filters.md). For the persisted
`ChunkFilterSpec` model, provider registry lifecycle, and runtime supplier
resolution, see
[Chunk Filter Provider Model](../architecture/chunk-filter-provider-model.md).

## Builder API

Filters are configured through `IndexConfigurationBuilder`.

Built-in or legacy class-based filters:

- `addEncodingFilter(ChunkFilter)`
- `addEncodingFilter(Class<? extends ChunkFilter>)`
- `addDecodingFilter(ChunkFilter)`
- `addDecodingFilter(Class<? extends ChunkFilter>)`
- `withEncodingFilters(Collection<ChunkFilter>)`
- `withDecodingFilters(Collection<ChunkFilter>)`
- `withEncodingFilterClasses(Collection<Class<? extends ChunkFilter>>)`
- `withDecodingFilterClasses(Collection<Class<? extends ChunkFilter>>)`

Provider-backed custom filters:

- `addEncodingFilter(Supplier<? extends ChunkFilter>, ChunkFilterSpec)`
- `addDecodingFilter(Supplier<? extends ChunkFilter>, ChunkFilterSpec)`
- `withEncodingFilterRegistrations(Collection<ChunkFilterRegistration>)`
- `withDecodingFilterRegistrations(Collection<ChunkFilterRegistration>)`

If you persist custom provider-backed filters, pass the matching
`ChunkFilterProviderRegistry` when calling:

- `SegmentIndex.create(directory, conf, registry)`
- `SegmentIndex.open(directory, conf, registry)`
- `SegmentIndex.open(directory, registry)`
- `SegmentIndex.tryOpen(directory, registry)`

## Which configuration style to choose

- Use `Class<? extends ChunkFilter>` for built-in filters and other no-arg
  filters.
- Use `ChunkFilter` instances only for shared filters that are safe to reuse.
- Use `Supplier<? extends ChunkFilter> + ChunkFilterSpec` when the filter needs
  runtime dependencies, constructor arguments, or fresh instances.

## Defaults

If you do not provide custom filters:

- Encoding defaults: `ChunkFilterCrc32Writing` ->
  `ChunkFilterMagicNumberWriting`
- Decoding defaults: `ChunkFilterMagicNumberValidation` ->
  `ChunkFilterCrc32Validation`

## Constraints

- Encoding and decoding filter lists must not be empty.
- Decoding order must mirror the inverse of encoding for reversible transforms.
- Persisted metadata stores `ChunkFilterSpec`, not suppliers or secrets.
- A custom `providerId` should identify one logical encode/decode pair.
- Reopening an index with custom filters requires the same provider id to be
  present in the supplied `ChunkFilterProviderRegistry`.

## Example: built-in filters by class

Enable Snappy compression with matching decode order:

```java
IndexConfiguration<Integer, String> conf = IndexConfiguration
    .<Integer, String>builder()
    .withKeyClass(Integer.class)
    .withValueClass(String.class)
    .withName("orders")
    .addEncodingFilter(ChunkFilterCrc32Writing.class)
    .addEncodingFilter(ChunkFilterMagicNumberWriting.class)
    .addEncodingFilter(ChunkFilterSnappyCompress.class)
    .addDecodingFilter(ChunkFilterMagicNumberValidation.class)
    .addDecodingFilter(ChunkFilterSnappyDecompress.class)
    .addDecodingFilter(ChunkFilterCrc32Validation.class)
    .build();
```

Add XOR on top of compression:

```java
builder
    .addEncodingFilter(ChunkFilterXorEncrypt.class)
    .addDecodingFilter(ChunkFilterXorDecrypt.class);
```

## Example: custom provider with Spring context

The provider owns runtime dependency lookup. The persisted spec carries only a
stable `providerId` and safe parameters such as `keyRef`.

### Spring-managed provider

```java
@Component
final class AesGcmChunkFilterProvider implements ChunkFilterProvider {

    private final SecretKeyResolver secretKeyResolver;

    AesGcmChunkFilterProvider(final SecretKeyResolver secretKeyResolver) {
        this.secretKeyResolver = secretKeyResolver;
    }

    @Override
    public String getProviderId() {
        return "aes-gcm";
    }

    @Override
    public Supplier<? extends ChunkFilter> createEncodingSupplier(
            final ChunkFilterSpec spec) {
        final String keyRef = spec.getRequiredParameter("keyRef");
        return () -> new AesGcmEncryptChunkFilter(
                secretKeyResolver.resolveRequired(keyRef));
    }

    @Override
    public Supplier<? extends ChunkFilter> createDecodingSupplier(
            final ChunkFilterSpec spec) {
        final String keyRef = spec.getRequiredParameter("keyRef");
        return () -> new AesGcmDecryptChunkFilter(
                secretKeyResolver.resolveRequired(keyRef));
    }
}
```

### Registry bean

```java
@Configuration
class ChunkFilterProviderConfiguration {

    @Bean
    ChunkFilterProviderRegistry chunkFilterProviderRegistry(
            final List<ChunkFilterProvider> customProviders) {
        ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .defaultRegistry();
        for (final ChunkFilterProvider provider : customProviders) {
            registry = registry.withProvider(provider);
        }
        return registry;
    }
}
```

### Create and reopen an index

```java
ChunkFilterSpec aesSpec = ChunkFilterSpec.ofProvider("aes-gcm")
    .withParameter("keyRef", "orders-main");

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("orders")
    .addEncodingFilter(ChunkFilterCrc32Writing.class)
    .addEncodingFilter(ChunkFilterMagicNumberWriting.class)
    .addEncodingFilter(ChunkFilterSnappyCompress.class)
    .addEncodingFilter(
            aesGcmChunkFilterProvider.createEncodingSupplier(aesSpec),
            aesSpec)
    .addDecodingFilter(ChunkFilterMagicNumberValidation.class)
    .addDecodingFilter(
            aesGcmChunkFilterProvider.createDecodingSupplier(aesSpec),
            aesSpec)
    .addDecodingFilter(ChunkFilterSnappyDecompress.class)
    .addDecodingFilter(ChunkFilterCrc32Validation.class)
    .build();

SegmentIndex<String, String> index = SegmentIndex.create(directory, conf,
        chunkFilterProviderRegistry);

SegmentIndex<String, String> reopened = SegmentIndex.open(directory,
        chunkFilterProviderRegistry);
```

In this example:

- `aesGcmChunkFilterProvider` is the Spring bean shown above
- `chunkFilterProviderRegistry` is the Spring bean that wraps the built-in
  providers and the custom AES provider
- the same `ChunkFilterSpec` is used for both encoding and decoding because the
  provider id describes one logical encode/decode pair

## Related docs

- [Filters & Integrity](../architecture/filters.md)
- [Chunk Filter Provider Model](../architecture/chunk-filter-provider-model.md)
- [Data Types](data-types.md)

Snappy project home: [snappy-java](https://github.com/xerial/snappy-java)
