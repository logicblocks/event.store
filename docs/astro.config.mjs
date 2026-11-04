// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { satteri } from '@astrojs/markdown-satteri';
import starlightLinksValidator from 'starlight-links-validator';
import { readdirSync } from 'node:fs';

const referenceGroup = {
	label: 'Reference',
	items: [{ autogenerate: { directory: 'reference' } }],
};

function hasReferencePages() {
	try {
		return readdirSync(new URL('./src/content/docs/reference/', import.meta.url)).some(
			(file) => /\.mdx?$/.test(file),
		);
	} catch {
		return false;
	}
}

const isDraft = process.env.DOCS_DRAFT === 'true';

export default defineConfig({
	site: 'https://logicblocks.github.io',
	base: '/event.store',
	markdown: {
		// The generated reference pages set heading ids with `{#id}`.
		processor: satteri({ features: { headingAttributes: true } }),
	},
	integrations: [
		starlight({
			title: 'logicblocks.event.store',
			components: isDraft ? { Banner: './src/components/DraftBanner.astro' } : {},
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/logicblocks/event.store',
				},
			],
			editLink: {
				baseUrl: 'https://github.com/logicblocks/event.store/edit/main/docs/',
			},
			// Applies Starlight's Markdown features to the root CHANGELOG and CONTRIBUTING pages.
			markdown: { processedDirs: ['..'] },
			plugins: [starlightLinksValidator()],
			sidebar: [
				{ label: 'Home', link: '/' },
				{ label: 'Getting started', slug: 'getting-started' },
				{
					label: 'Guides',
					items: [{ label: 'Finalising state', slug: 'guides/finalising-state' }],
				},
				...(hasReferencePages() ? [referenceGroup] : []),
				{ label: 'Changelog', slug: 'changelog' },
				{ label: 'Contributing', slug: 'contributing' },
			],
		}),
	],
});
