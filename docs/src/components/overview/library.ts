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

export type KeyName = { name: string; what: string };

export type Package = {
	id: PackageId;
	title: string;
	tagline: string;
	how: string[];
	names: KeyName[];
	dependsOn: PackageId[];
	guides: { label: string; href: string }[];
};

const base = '/event.store';

export const packages: Record<PackageId, Package> = {
	types: {
		id: 'types',
		title: 'Types',
		tagline: 'The vocabulary every other package speaks.',
		how: [
			'Frozen dataclasses for the things the library passes around: the events you publish and read back, the identifiers that say where events live, and projections.',
			'JSON protocols let your own payload and state classes be stored and read back.',
		],
		names: [
			{ name: 'NewEvent', what: 'An event you are about to publish' },
			{ name: 'StoredEvent', what: 'An event as recorded, with id, position and sequence number' },
			{ name: 'StreamIdentifier', what: 'Says where events live. Also CategoryIdentifier and LogIdentifier' },
			{ name: 'Projection', what: 'State folded from events, with an id, name, source and metadata' },
			{ name: 'JsonValueConvertible', what: 'Implement it to store your own types' },
		],
		dependsOn: ['utils'],
		guides: [
			{ label: 'New events and stored events', href: `${base}/guides/streams-categories-and-logs/#new-events-and-stored-events` },
			{ label: 'Custom payload and state types', href: `${base}/guides/serialisation/` },
		],
	},
	utils: {
		id: 'utils',
		title: 'Utils',
		tagline: 'Clocks, so time can be controlled.',
		how: [
			'The library reads the time through a clock. It defaults to the system clock, and you can swap in a static clock to make timestamps predictable in tests.',
		],
		names: [
			{ name: 'Clock', what: 'The interface for reading the current time' },
			{ name: 'SystemClock', what: 'The real time. The default everywhere' },
			{ name: 'StaticClock', what: 'A fixed time you move on with set()' },
		],
		dependsOn: [],
		guides: [{ label: 'Clocks', href: `${base}/guides/testing/#clocks` }],
	},
	query: {
		id: 'query',
		title: 'Query',
		tagline: 'Describes searches without tying them to a database.',
		how: [
			'Queries are plain values: filters, a sort and a page. Each storage adapter translates them into its own language, such as SQL for Postgres or Python functions in memory.',
		],
		names: [
			{ name: 'FilterClause', what: 'An Operator applied to a Path, such as state.dob ≥ 1990' },
			{ name: 'SortClause', what: 'Sort by one or more fields' },
			{ name: 'KeySetPagingClause', what: 'Paging that stays stable as data changes. Also OffsetPagingClause' },
			{ name: 'Lookup', what: 'A query for one match. Search finds many' },
		],
		dependsOn: [],
		guides: [{ label: 'Searching projections', href: `${base}/guides/projection-store/#searching` }],
	},
	sources: {
		id: 'sources',
		title: 'Sources',
		tagline: 'One way to read events, wherever they come from.',
		how: [
			'An event source is anything you can read events from: a stream, a category, the whole log, or a list held in memory.',
			'They all read the same way, with async for, read(), iterate() and latest(). Constraints such as sequence_number_after let a reader carry on from where it stopped.',
		],
		names: [
			{ name: 'EventSource', what: 'The shared reading interface' },
			{ name: 'constraints', what: 'Filters such as sequence_number_after(n)' },
			{ name: 'EventSourceFactory', what: 'Builds a source from an identifier. Used by subscribers' },
			{ name: 'InMemoryEventSource', what: 'Base class for sources held in memory' },
		],
		dependsOn: ['types'],
		guides: [{ label: 'Reading events', href: `${base}/guides/streams-categories-and-logs/#reading-events` }],
	},
	persistence: {
		id: 'persistence',
		title: 'Persistence',
		tagline: 'Plumbing shared by the storage adapters.',
		how: [
			'Converters translate queries and constraints into each backend’s language. The Postgres module holds connection settings and table settings.',
			'You mostly touch it to connect to Postgres or to write your own adapter.',
		],
		names: [
			{ name: 'postgres.ConnectionSettings', what: 'Host, port, database and credentials' },
			{ name: 'postgres.TableSettings', what: 'Which table an adapter uses' },
			{ name: 'TypeRegistryConverter', what: 'Picks the converter registered for a query, constraint or condition type' },
		],
		dependsOn: ['query', 'types', 'utils'],
		guides: [
			{ label: 'Using PostgreSQL', href: `${base}/guides/postgres/` },
			{ label: 'Writing a storage adapter', href: `${base}/guides/custom-storage-adapters/` },
		],
	},
	store: {
		id: 'store',
		title: 'Store',
		tagline: 'Appends events and reads them back.',
		how: [
			'An EventStore gives you the log, a category or a stream. None of these calls touch storage, and none of them need creating first.',
			'publish appends events atomically, optionally guarded by a write condition. A storage adapter does the work: in memory for tests, Postgres for production.',
		],
		names: [
			{ name: 'EventStore', what: 'The entry point: .log(), .category(), .stream()' },
			{ name: 'EventStream', what: 'One thing’s events. Publish and read. Also EventCategory and EventLog' },
			{ name: 'conditions', what: 'position_is, stream_is_empty, combined with & and |' },
			{ name: 'retry_on_unmet_condition_error', what: 'Re-runs read, decide, publish after a conflict' },
			{ name: 'InMemoryEventStorageAdapter', what: 'For tests. PostgresEventStorageAdapter for production' },
		],
		dependsOn: ['sources', 'persistence', 'types'],
		guides: [
			{ label: 'Streams, categories and the log', href: `${base}/guides/streams-categories-and-logs/` },
			{ label: 'Concurrency and write conditions', href: `${base}/guides/concurrency/` },
		],
	},
	projection: {
		id: 'projection',
		title: 'Projection',
		tagline: 'Folds events into state, and keeps that state.',
		how: [
			'A Projector is a fold. You give it an initial state and one handler per event name: item-added is handled by item_added. project() runs every event in a source through the handlers.',
			'A ProjectionStore saves the result, loads it by id or source, and searches it with the query language.',
		],
		names: [
			{ name: 'Projector', what: 'Your subclass: initial state, handlers, id_factory' },
			{ name: 'ProjectionStore', what: 'save, load, locate and search' },
			{ name: 'InMemoryProjectionStorageAdapter', what: 'For tests. PostgresProjectionStorageAdapter for production' },
			{ name: 'MissingHandlerBehaviour', what: 'Whether events without a handler raise or are ignored' },
		],
		dependsOn: ['sources', 'query', 'persistence', 'types'],
		guides: [
			{ label: 'Storing and querying projections', href: `${base}/guides/projection-store/` },
			{ label: 'Finalising state', href: `${base}/guides/finalising-state/` },
		],
	},
	processing: {
		id: 'processing',
		title: 'Processing',
		tagline: 'Does work as events arrive, on one node or many.',
		how: [
			'A subscriber reads an event source and passes each event to your processor, saving a checkpoint as it goes. A broker decides which subscriber reads which source.',
			'Services run all of this in the background, with polling, retries and clean shutdown.',
		],
		names: [
			{ name: 'EventProcessor', what: 'Your code: process_event(event)' },
			{ name: 'ProjectionEventProcessor', what: 'Keeps a projection store up to date' },
			{ name: 'make_event_store_subscriber', what: 'Reads a source, calls the processor, checkpoints' },
			{ name: 'make_event_broker', what: 'Singleton or distributed, in memory or Postgres' },
			{ name: 'ServiceManager', what: 'Runs services. Also PollingService and ErrorHandlingService' },
		],
		dependsOn: ['store', 'projection', 'sources', 'persistence', 'query', 'types', 'utils'],
		guides: [
			{ label: 'Processing events asynchronously', href: `${base}/guides/event-processing/` },
			{ label: 'Services and error handling', href: `${base}/guides/services-and-error-handling/` },
			{ label: 'How the distributed broker works', href: `${base}/concepts/distributed-event-broker/` },
		],
	},
	testing: {
		id: 'testing',
		title: 'Testing',
		tagline: 'Builders and random data for your tests.',
		how: [
			'Immutable builders make events and projections, filling every field you don’t set with random data. Combine them with the in-memory adapters and a static clock for fast, predictable tests.',
		],
		names: [
			{ name: 'NewEventBuilder', what: 'Builds NewEvents' },
			{ name: 'StoredEventBuilder', what: 'Builds StoredEvents, optionally .from_new_event()' },
			{ name: 'MappingProjectionBuilder', what: 'Builds Projections. Subclass BaseProjectionBuilder for your own' },
			{ name: 'data', what: 'Random names, ids, payloads and timestamps' },
		],
		dependsOn: ['types', 'utils'],
		guides: [{ label: 'Testing', href: `${base}/guides/testing/` }],
	},
};

export const usedBy = (id: PackageId): PackageId[] =>
	(Object.keys(packages) as PackageId[]).filter((other) => packages[other].dependsOn.includes(id));

export const layers: { label: string; detail: string; packages: PackageId[] }[] = [
	{ label: 'Background', detail: 'React to events as they arrive', packages: ['processing'] },
	{ label: 'State', detail: 'Turn events into answers', packages: ['projection'] },
	{ label: 'Events', detail: 'Append and read facts', packages: ['store'] },
	{ label: 'Plumbing', detail: 'Reading and storage machinery', packages: ['sources', 'persistence'] },
	{ label: 'Foundations', detail: 'Shared vocabulary', packages: ['types', 'query', 'utils'] },
];

export const packageOrder: PackageId[] = [
	'processing',
	'projection',
	'store',
	'sources',
	'persistence',
	'types',
	'query',
	'utils',
	'testing',
];
