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
`ChunkFilterSpec` model, provider resolver lifecycle, and runtime supplier
resolution, see
[Chunk Filter Provider Model](../architecture/chunk-filter-provider-model.md).

## Builder API

Filters are configured through the grouped
`IndexConfiguration.builder().filters(...)` section.

Built-in or legacy class-based filters:

- `filters(...).addEncodingFilter(ChunkFilter)`
- `filters(...).addEncodingFilter(Class<? extends ChunkFilter>)`
- `filters(...).addDecodingFilter(ChunkFilter)`
- `filters(...).addDecodingFilter(Class<? extends ChunkFilter>)`
- `filters(...).encodingFilters(Collection<ChunkFilter>)`
- `filters(...).decodingFilters(Collection<ChunkFilter>)`
- `filters(...).encodingFilterClasses(Collection<Class<? extends ChunkFilter>>)`
- `filters(...).decodingFilterClasses(Collection<Class<? extends ChunkFilter>>)`

Provider-backed custom filters:

- `filters(...).addEncodingFilter(ChunkFilterSpec)`
- `filters(...).addDecodingFilter(ChunkFilterSpec)`
- `filters(...).encodingFilterRegistrations(Collection<ChunkFilterRegistration>)`
- `filters(...).decodingFilterRegistrations(Collection<ChunkFilterRegistration>)`

If you persist custom provider-backed filters, pass the matching
`ChunkFilterProviderResolver` either in the filter configuration or when
calling:

- `filters(...).chunkFilterProviderResolver(resolver)`
- `SegmentIndex.create(directory, conf, resolver)`
- `SegmentIndex.open(directory, conf, resolver)`
- `SegmentIndex.open(directory, resolver)`
- `SegmentIndex.tryOpen(directory, resolver)`

## Which configuration style to choose

- Use `Class<? extends ChunkFilter>` for built-in filters and other no-arg
  filters.
- Use `ChunkFilter` instances only for shared filters that are safe to reuse.
- Use `ChunkFilterSpec` with a matching `ChunkFilterProviderResolver` when the
  filter needs runtime dependencies, constructor arguments, or fresh instances
  such as `ChunkFilterAesGcmEncrypt` and `ChunkFilterAesGcmDecrypt`.

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
  present in the supplied `ChunkFilterProviderResolver`.

## Example: built-in filters by class

Enable Snappy compression with matching decode order:

```java
IndexConfiguration<Integer, String> conf = IndexConfiguration
    .<Integer, String>builder()
    .identity(identity -> identity
        .name("orders")
        .keyClass(Integer.class)
        .valueClass(String.class))
    .filters(filters -> filters
        .addEncodingFilter(ChunkFilterCrc32Writing.class)
        .addEncodingFilter(ChunkFilterMagicNumberWriting.class)
        .addEncodingFilter(ChunkFilterSnappyCompress.class)
        .addDecodingFilter(ChunkFilterMagicNumberValidation.class)
        .addDecodingFilter(ChunkFilterSnappyDecompress.class)
        .addDecodingFilter(ChunkFilterCrc32Validation.class))
    .build();
```

Use XOR when you only need reversible obfuscation:

```java
IndexConfiguration<Integer, String> conf = IndexConfiguration
    .<Integer, String>builder()
    .identity(identity -> identity
        .name("orders")
        .keyClass(Integer.class)
        .valueClass(String.class))
    .filters(filters -> filters
        .addEncodingFilter(ChunkFilterXorEncrypt.class)
        .addDecodingFilter(ChunkFilterXorDecrypt.class))
    .build();
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
        return () -> new ChunkFilterAesGcmEncrypt(
                secretKeyResolver.resolveRequired(keyRef));
    }

    @Override
    public Supplier<? extends ChunkFilter> createDecodingSupplier(
            final ChunkFilterSpec spec) {
        final String keyRef = spec.getRequiredParameter("keyRef");
        return () -> new ChunkFilterAesGcmDecrypt(
                secretKeyResolver.resolveRequired(keyRef));
    }
}
```

### Resolver bean

```java
@Configuration
class ChunkFilterProviderConfiguration {

    @Bean
    ChunkFilterProviderResolver chunkFilterProviderResolver(
            final List<ChunkFilterProvider> customProviders) {
        ChunkFilterProviderResolverImpl resolver = ChunkFilterProviderResolverImpl
                .defaultResolver();
        for (final ChunkFilterProvider provider : customProviders) {
            resolver = resolver.withProvider(provider);
        }
        return resolver;
    }
}
```

### Create and reopen an index

```java
ChunkFilterSpec aesSpec = ChunkFilterSpec.ofProvider("aes-gcm")
    .withParameter("keyRef", "orders-main");

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .identity(identity -> identity
        .name("orders")
        .keyClass(String.class)
        .valueClass(String.class))
    .filters(filters -> filters
        .chunkFilterProviderResolver(chunkFilterProviderResolver)
        .addEncodingFilter(ChunkFilterCrc32Writing.class)
        .addEncodingFilter(ChunkFilterMagicNumberWriting.class)
        .addEncodingFilter(ChunkFilterSnappyCompress.class)
        .addEncodingFilter(aesSpec)
        .addDecodingFilter(ChunkFilterMagicNumberValidation.class)
        .addDecodingFilter(aesSpec)
        .addDecodingFilter(ChunkFilterSnappyDecompress.class)
        .addDecodingFilter(ChunkFilterCrc32Validation.class))
    .build();

SegmentIndex<String, String> index = SegmentIndex.create(directory, conf);

SegmentIndex<String, String> reopened = SegmentIndex.open(directory,
        chunkFilterProviderResolver);
```

In this example:

- `aesGcmChunkFilterProvider` is the Spring bean shown above
- `chunkFilterProviderResolver` is the Spring bean that wraps the built-in
  providers and the custom AES provider
- the same `ChunkFilterSpec` is used for both encoding and decoding because the
  provider id describes one logical encode/decode pair
- `ChunkFilterAesGcmEncrypt` and `ChunkFilterAesGcmDecrypt` are not part of the
  default resolver because they require an application-managed `SecretKey`

## Related docs

- [Filters & Integrity](../architecture/filters.md)
- [Chunk Filter Provider Model](../architecture/chunk-filter-provider-model.md)
- [Data Types](data-types.md)

Snappy project home: [snappy-java](https://github.com/xerial/snappy-java)
