# Apache Polaris: catalog federation fails with HTTP 500 when the remote answers with brotli

**Project:** Apache Polaris (Docker image `apache/polaris:1.9.0`, linux/amd64; found with 1.7.0)
**Severity:** feature unusable against remote catalogs that compress with brotli

## Summary

With `ENABLE_CATALOG_FEDERATION=true`, every request to an EXTERNAL catalog of type
`ICEBERG_REST` fails with HTTP 500 when the remote Iceberg REST server answers with
`Content-Encoding: br`. Polaris' Iceberg REST client (Apache HttpClient 5) advertises brotli,
but the image contains only `brotli4j-1.24.0.jar` and `service-1.24.0.jar` (1.7.0: 1.23.0) - no
native library jar for linux-x86_64 - so decoding the response throws `UnsatisfiedLinkError`.

## Steps to reproduce

1. Start `apache/polaris:1.9.0` with
   `polaris.features."ENABLE_CATALOG_FEDERATION"=true`,
   `polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"=["ICEBERG_REST"]`,
   `polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"=["OAUTH","BEARER","SIGV4"]`.
2. Create an EXTERNAL catalog whose `connectionConfigInfo.uri` points at a REST catalog that
   supports brotli (here: Lakekeeper v0.15.0, which answers `Accept-Encoding: br` with
   `Content-Encoding: br`).
3. `GET /api/catalog/v1/<catalog>/namespaces` (or load a table).

## Expected

The remote's answer is decoded (or Polaris does not advertise brotli).

## Actual

HTTP 500 to the client (`{"details":"Error id ..."}`); server log:

```
java.lang.UnsatisfiedLinkError: 'java.nio.ByteBuffer com.aayushatharva.brotli4j.decoder.DecoderJNI.nativeCreate(long[])'
  at com.aayushatharva.brotli4j.decoder.DecoderJNI.nativeCreate(Native Method)
  ...
  at org.apache.hc.client5.http.entity.compress.DecompressingEntity$LazyDecompressingInputStream.getDecompressedStream(DecompressingEntity.java:121)
```

## Notes

- Through a proxy that forces `Accept-Encoding: identity`, federation works: read, append,
  create and drop all reach the remote catalog.
- Fix options: ship `com.aayushatharva.brotli4j:native-linux-x86_64` (and aarch64) in the
  image, or disable brotli in the federation HTTP client.

Lab test: `tests/catalog/test_catalog_features.py::test_federation_direct` (CATALOG=polaris).
