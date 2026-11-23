export type PackageId =
	| 'types'
	| 'utils'
	| 'query'
	| 'sources'
	| 'persistence'
	| 'store'
	| 'projection'
	| 'processing'
	| 'testing';

export const base = '/event.store';

export const journeyPackages: PackageId[] = [
	'types',
	'store',
	'sources',
	'persistence',
	'projection',
	'query',
	'processing',
];
