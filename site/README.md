# Tallyo GitHub Pages Site

This folder is ready for GitHub Pages hosting with custom domain `tallyo.net`.

## What is included

- `/` homepage
- `/privacy/` (from `docs/legal/PRIVACY_POLICY.md`)
- `/terms/` (from `docs/legal/TERMS_OF_SERVICE.md`)
- `CNAME` set to `tallyo.net`
- GitHub Actions workflow at `.github/workflows/deploy-pages.yml`

## Publish steps

1. Push this repository to GitHub.
2. In GitHub repo settings:
   - Open `Settings > Pages`
   - Source: `GitHub Actions`
3. In your DNS provider for `tallyo.net`, set:
   - `A` -> `185.199.108.153`
   - `A` -> `185.199.109.153`
   - `A` -> `185.199.110.153`
   - `A` -> `185.199.111.153`
4. Wait DNS propagation, then verify:
   - `https://tallyo.net/`
   - `https://tallyo.net/privacy/`
   - `https://tallyo.net/terms/`

## Keep legal pages in sync

If you update legal docs in `docs/legal`, regenerate pages:

```bash
{
  printf -- '---\nlayout: default\ntitle: Privacy Policy\npermalink: /privacy/\n---\n\n';
  cat docs/legal/PRIVACY_POLICY.md;
} > site/privacy.md

{
  printf -- '---\nlayout: default\ntitle: Terms of Service\npermalink: /terms/\n---\n\n';
  cat docs/legal/TERMS_OF_SERVICE.md;
} > site/terms.md
```
