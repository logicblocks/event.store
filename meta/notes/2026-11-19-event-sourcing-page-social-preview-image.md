---
type: note
id: "2026-11-19-event-sourcing-page-social-preview-image"
title: "Social preview image for the event-sourcing page"
date: "2026-11-19T14:56:09+00:00"
author: "Joe Gambino"
producer: create-note
status: captured
topic: "Social preview / thumbnail image for the event-sourcing page"
tags: [docs, social-preview, open-graph, event-sourcing-page]
revision: "6946dceb2f0d4cc31df84abf7aa2ae0756a108c7"
repository: "event.store"
last_updated: "2026-11-19T14:56:09+00:00"
last_updated_by: "Joe Gambino"
schema_version: 1
---

# Social preview image for the event-sourcing page

## The images

All images are captures of the right-hand **event stream** pane of
`OverviewAnimation` (`docs/src/components/event-sourcing/OverviewAnimation.astro`),
taken at the animation's final state: all five events for `cart-42` (#0–#4,
three `item-added`, one `item-removed`, one `checked-out`) with the caption
"A cart is represented by an append only stream of immutable facts." and no
leftover highlight from the mug step.

| Image | Where | Size | Purpose |
|-------|-------|------|---------|
| Social card | `docs/public/event-sourcing-social.png` (in the repo) | 1200×630, opaque, ~85 KB | The page's `og:image`: the link preview shown by Slack, LinkedIn, Twitter/X, etc. |
| Transparent pane | not kept in the repo | 1030×642 (2× scale), alpha | The pane on its own with transparent rounded corners, to reuse on other backgrounds. Its panel fill is slightly translucent (alpha ≈ 228), so it looks a little different on backgrounds other than the site's dark one. |
| Light/dark pane screenshots | not kept in the repo | 1030×642 | The first captures, with the page background showing in the corners; replaced by the two above. |

The social card is the transparent pane scaled to 518 px tall, centred with
56 px padding top and bottom on the site's dark page background
(`rgb(14, 15, 25)`), then flattened to remove the alpha channel.

## Why dark only

Open Graph allows one `og:image` per page, and services that fetch it don't
know whether the viewer is in light or dark mode. Light/dark variants are only
possible for things like favicons (via `media` on `<link>`), not social
previews. We chose dark.

## How it's wired in

The `head:` block in the frontmatter of
`docs/src/content/docs/event-sourcing.mdx` adds `og:image`,
`og:image:width`, `og:image:height` and `og:image:alt`. Things to know:

- `og:image` must be an **absolute** URL, so it points at the live site
  (`https://logicblocks.github.io/event.store/event-sourcing-social.png`). It
  won't resolve until the docs are deployed; check it afterwards with a tool
  like opengraph.xyz.
- Starlight already emits `twitter:card: summary_large_image` on every page,
  and Twitter/X falls back to `og:image`, so no `twitter:image` tag is needed.
- The site has `base: '/event.store'`, so files in `docs/public/` are served
  under `/event.store/…`. The dev server returns 404 for paths without the
  base (e.g. `/event-sourcing/`).

Unresolved: the page `description` contains Markdown (`**_what happened_**`),
which flows into `og:description` verbatim, so previews show the asterisks and
underscores.

## How to recreate

1. Start the docs dev server (`npm run dev` in `docs/`, default port 4321).
2. In a throwaway directory outside the repo, install the tools:
   `npm init -y && npm i playwright sharp`, then `npx playwright install
   chromium` (or set `CHROMIUM_PATH` to an existing headless Chromium, e.g.
   under `~/Library/Caches/ms-playwright/`).
3. Save the script below as `card.mjs` and run
   `node card.mjs <output-dir>`. It writes `pane-transparent.png` and
   `social-card.png` to the output directory.
4. Copy `social-card.png` over `docs/public/event-sourcing-social.png`.

The script waits for the stream punchline to become visible (about 15 s into
the animation), then a further 1.2 s for transitions to settle. If the
animation's markup changes, update the selectors: `[data-es-overview]`,
`[data-punchline="stream"].is-visible` and `.es-overview__pane--stream`.

```js
import { chromium } from 'playwright';
import sharp from 'sharp';

const [out] = process.argv.slice(2);
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 }, deviceScaleFactor: 2, colorScheme: 'dark' });
await page.addInitScript(() => localStorage.setItem('starlight-theme', 'dark'));
await page.goto('http://localhost:4321/event.store/event-sourcing/');
await page.waitForTimeout(1500);
await page.evaluate(() => document.querySelector('[data-es-overview]').scrollIntoView({ block: 'center' }));
await page.locator('[data-punchline="stream"].is-visible').waitFor({ timeout: 60000 });
await page.waitForTimeout(1200);

const pageBackground = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
const pane = page.locator('.es-overview__pane--stream');
await pane.evaluate((el) => {
  for (let node = el.parentElement; node; node = node.parentElement) node.style.setProperty('background', 'transparent', 'important');
});
await pane.screenshot({ path: `${out}/pane-transparent.png`, omitBackground: true, animations: 'disabled' });
await browser.close();

const [width, height, padding] = [1200, 630, 56];
const scaled = await sharp(`${out}/pane-transparent.png`).resize({ height: height - 2 * padding }).toBuffer();
const { width: paneWidth } = await sharp(scaled).metadata();
const card = await sharp({ create: { width, height, channels: 4, background: pageBackground } })
  .composite([{ input: scaled, left: Math.round((width - paneWidth) / 2), top: padding }])
  .png()
  .toBuffer();
await sharp(card).flatten({ background: pageBackground }).png({ compressionLevel: 9 }).toFile(`${out}/social-card.png`);
```

For a light-theme capture, change `colorScheme` and the `starlight-theme`
value to `'light'`.
