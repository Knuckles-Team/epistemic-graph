# Swagger UI

Interactive browser for [`openapi.json`](openapi.json), generated from the
canonical contract registry by `scripts/gen_api_docs.py` (see the
[API reference](api/index.md) for the same facts as Markdown). Rendered with a
locally vendored Swagger UI (`docs/assets/swagger-ui/`, Apache-2.0, see its
`NOTICE.md`) — no CDN, no external network request.

Every request/response body here is shown as its JSON-Schema-equivalent
shape; the real wire encoding is MessagePack, not JSON — see `openapi.json`'s
own `info.description`.

<div id="swagger-ui"></div>

<link rel="stylesheet" href="../assets/swagger-ui/swagger-ui.css">
<script src="../assets/swagger-ui/swagger-ui-bundle.js"></script>
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
