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
				{ label: 'Introduction to event sourcing', slug: 'event-sourcing' },
				{ label: 'Library overview', slug: 'overview' },
				{ label: 'Getting started', slug: 'getting-started' },
				{
					label: 'Guides',
					items: [
						{ label: 'Streams, categories and the log', slug: 'guides/streams-categories-and-logs' },
						{ label: 'Concurrency and write conditions', slug: 'guides/concurrency' },
						{ label: 'Storing and querying projections', slug: 'guides/projection-store' },
						{ label: 'Finalising state', slug: 'guides/finalising-state' },
						{ label: 'Processing events asynchronously', slug: 'guides/event-processing' },
						{ label: 'Services and error handling', slug: 'guides/services-and-error-handling' },
						{ label: 'Using PostgreSQL', slug: 'guides/postgres' },
						{ label: 'Custom payload and state types', slug: 'guides/serialisation' },
						{ label: 'Testing', slug: 'guides/testing' },
						{ label: 'Logging', slug: 'guides/logging' },
						{ label: 'Writing a storage adapter', slug: 'guides/custom-storage-adapters' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{
							label: 'How the distributed event broker works',
							slug: 'concepts/distributed-event-broker',
						},
					],
				},
				...(hasReferencePages() ? [referenceGroup] : []),
				{ label: 'Changelog', slug: 'changelog' },
				{ label: 'Contributing', slug: 'contributing' },
			],
		}),
	],
});
