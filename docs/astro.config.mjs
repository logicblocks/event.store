// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

export default defineConfig({
	site: 'https://logicblocks.github.io',
	base: '/event.store',
	integrations: [
		starlight({
			title: 'logicblocks.event.store',
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
			plugins: [starlightLinksValidator()],
			sidebar: [
				{ label: 'Home', link: '/' },
				{ label: 'Getting started', slug: 'getting-started' },
			],
		}),
	],
});
