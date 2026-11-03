import { defineCollection } from 'astro:content';
import { docsSchema } from '@astrojs/starlight/schema';
import { docsWithRootPagesLoader } from './loaders';

export const collections = {
	docs: defineCollection({
		loader: docsWithRootPagesLoader([
			{
				id: 'changelog',
				file: 'CHANGELOG.md',
				title: 'Changelog',
				description: 'Notable changes in each release.',
			},
			{
				id: 'contributing',
				file: 'CONTRIBUTING.md',
				title: 'Contributing',
				description: 'How to contribute to logicblocks.event.store.',
			},
		]),
		schema: docsSchema(),
	}),
};
