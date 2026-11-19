import { readFile } from 'node:fs/promises';
import type { Loader, LoaderContext } from 'astro/loaders';
import { docsLoader } from '@astrojs/starlight/loaders';

const repositoryUrl = 'https://github.com/logicblocks/event.store';
const docsCollectionPath = 'src/content/docs';

export interface RootPage {
	id: string;
	file: string;
	title: string;
	description?: string;
}

export function docsWithRootPagesLoader(pages: RootPage[]): Loader {
	const docs = docsLoader();
	return {
		name: 'docs-with-root-pages-loader',
		async load(context) {
			await docs.load(context);
			await Promise.all(pages.map((page) => loadRootPage(context, page)));
		},
	};
}

async function loadRootPage(context: LoaderContext, page: RootPage) {
	const sourceUrl = new URL(`../${page.file}`, context.config.root);
	const source = await readFile(sourceUrl, 'utf-8');
	const body = fitToPage(source);
	const filePath = `${docsCollectionPath}/${page.id}.md`;
	const data = await context.parseData({
		id: page.id,
		data: {
			title: page.title,
			description: page.description,
			editUrl: `${repositoryUrl}/edit/main/${page.file}`,
		},
	});
	context.store.set({
		id: page.id,
		data,
		body,
		filePath,
		digest: context.generateDigest(source),
		// The slug lets the links validator key this page by its route, not by the source path.
		rendered: await context.renderMarkdown(
			`---\nslug: ${page.id}\n---\n${body}`,
			{ fileURL: sourceUrl },
		),
	});
}

function fitToPage(markdown: string): string {
	return linkRelativePathsToRepository(
		demoteHeadings(withoutLeadingTitle(markdown)),
	);
}

function withoutLeadingTitle(markdown: string): string {
	return markdown.replace(/^\s*# .*\n/, '');
}

function demoteHeadings(markdown: string): string {
	const lines = markdown.split('\n');
	const outsideCode = linesOutsideCodeFences(lines);
	if (!lines.some((line, index) => outsideCode[index] && /^# /.test(line))) {
		return markdown;
	}
	return lines
		.map((line, index) =>
			outsideCode[index] && /^#{1,5} /.test(line) ? `#${line}` : line,
		)
		.join('\n');
}

function linesOutsideCodeFences(lines: string[]): boolean[] {
	let inFence = false;
	return lines.map((line) => {
		if (/^\s*(```|~~~)/.test(line)) {
			inFence = !inFence;
			return false;
		}
		return !inFence;
	});
}

function linkRelativePathsToRepository(markdown: string): string {
	return markdown.replace(
		/\]\((?![a-z][a-z0-9+.-]*:|#|\/)([^)\s]+)\)/gi,
		(_, path: string) => `](${repositoryUrl}/blob/main/${path})`,
	);
}
