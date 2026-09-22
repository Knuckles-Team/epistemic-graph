# Vendored Swagger UI assets

`swagger-ui-bundle.js` and `swagger-ui.css` are vendored, unmodified, from the
[`swagger-ui`](https://github.com/swagger-api/swagger-ui) project (Apache
License 2.0), version **4.15.5**, via the
[`swagger-ui-bundle`](https://pypi.org/project/swagger-ui-bundle/) PyPI
package (`swagger_ui_bundle==1.1.0`, which mirrors the same npm
`swagger-ui-dist@4.15.5` release). Extracted with:

```bash
pip download --no-deps -d /tmp/swui swagger-ui-bundle
# then unzip the wheel's swagger_ui_bundle/vendor/swagger-ui-4.15.5/
# {swagger-ui-bundle.js, swagger-ui.css} into this directory.
```

Vendored (rather than loaded from a CDN) so the generated `docs/swagger-ui.md`
page works with no external network dependency, per this repository's Pages
theme convention (`INHERIT`ed `templates/mkdocs-theme/base.mkdocs.yml` in the
`pipelines` repo already forbids CDN-hosted content for the shared theme).

`.map` sourcemaps and the `swagger-ui-standalone-preset.js` topbar/URL-bar
chrome (which defaults its "Explore" field to
`https://petstore.swagger.io/v2/swagger.json`) are deliberately **not**
vendored — `docs/swagger-ui.md` uses the base layout only and always points
`SwaggerUIBundle` at the co-located `openapi.json`, so nothing in this page
ever references an external host.

Not managed by `scripts/gen_api_docs.py` — these are static, hand-vendored
assets, not derived from `contract/`. Re-vendor by repeating the steps above
when a newer Swagger UI release is wanted; update the version pin here and in
`docs/swagger-ui.md`'s comment when you do.
