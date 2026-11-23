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

export const corePackages: PackageId[] = ['types', 'store', 'projection', 'query', 'processing'];
