# Swagger UI

Interactive browser for [`openapi.json`](openapi.json), generated from the
canonical contract registry by `scripts/gen_api_docs.py` (see the
[API reference](api/index.md) for the same facts as Markdown). Rendered with
Swagger UI **4.15.5** (Apache-2.0) loaded from jsDelivr
(`swagger-ui-dist@4.15.5`), pinned to that exact version with
Subresource Integrity hashes on both the JS and the CSS, so the loaded
bytes are cryptographically guaranteed to be the same release this page was
built against.

Previously vendored byte-for-byte under `docs/assets/swagger-ui/`; moved to
a pinned, integrity-checked CDN reference instead because a minified
third-party bundle checked into this repository cannot carry an inline
provenance/exemption marker, which made it an unfixable false positive for
this repository's secret-history scanner. See `git log` for that vendored
history if the previous approach is ever needed again.

Every request/response body here is shown as its JSON-Schema-equivalent
shape; the real wire encoding is MessagePack, not JSON — see `openapi.json`'s
own `info.description`.

<div id="swagger-ui"></div>

<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui.css"
      integrity="sha384-2/StnWvcTFa+ulN5XGsmRCRCHlS3w55zYM2opgTX9cGDkOHlC2PJMND08SWG4Bag"
      crossorigin="anonymous">
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui-bundle.js"
        integrity="sha384-GJoyyEnbeIyINXWDkEzUHpPPCZPcP2KrAg83c6DGAkTPr2tDHQ59DuqMRwAwsJwV"
        crossorigin="anonymous"></script>
<script>
  window.addEventListener("DOMContentLoaded", function () {
    window.ui = SwaggerUIBundle({
      url: "../openapi.json",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis],
      layout: "BaseLayout",
      deepLinking: true,
    });
  });
</script>
