import { Graph, Node } from 'graph-crdt';

import * as pipeline from '../pipeline';
import merge from '../merge-configs';
import Context from './context';

const settings = Symbol('database configuration');

/**
 * Determines whether a connection is offline.
 * @param  {Connection} connection - Valid member of a connection group.
 * @return {Boolean} - Whether the connection is offline.
 */
const isOffline = (connection) => Boolean(connection.offline);

/**
 * Initiates a read from both storage and the network.
 * @param  {Database} db - Database context.
 * @param  {Object} action - Generated read action.
 * @return {Promise<Node|null>} - An aggregate node of all responses.
 */
const readFromPlugins = async (db, action) => {
  let node = null;

  // Ask the storage plugins for it.
  const reads = [...action.storage].map(store => store.read(action));

  // Ask the network for it.
  if (db.router) {
    reads.push(db.router.pull(action));
  }

  // Merge every response into the same node.
  for (const result of await Promise.all(reads)) {
    const update = Node.source(result);

    if (result) {
      node = node || new Context(db, { uid: action.key });
      node.merge(update);
    }
  }

  return node;
};

/**
 * Plugin manager for graph-crdt.
 * @class Database
 */
class Database extends Graph {

  /**
   * Instantiates a new graph database.
   * @param  {Object} config - A database configuration object.
   */
  constructor (config) {
    super();

    this[settings] = config;

    const createRouter = config.router;

    const router = createRouter
      ? createRouter(this, config)
      : null;

    Object.defineProperty(this, 'router', { value: router });

    const extensions = config.extend.root;

    /** Add API extensions from the config. */
    Object.keys(extensions).forEach((key) => {
      const value = extensions[key];

      /** Non-enumerable and immutable. */
      Object.defineProperty(this, key, { value });
    });
  }

  /**
   * Creates an in-memory database to aggregate changes before committing.
   * Storage and network plugins are excluded from the branch.
   * @return {Database} - A new, ephemeral database instance.
   */
  branch () {
    const { extend, hooks } = this[settings];

    const config = merge([{ extend, hooks }]);
    const branch = new Database(config);

    branch.merge(this);

    return branch;
  }

  /**
   * Applies a collection of changes all at once.
   * @param  {Graph|Database} update - A collection of updates.
   * @param  {Object} [options] - Override global configuration.
   * @return {Promise} - Resolves when the commit has been processed.
   */
  async commit (update, options = {}) {

    // Storage plugins get the full state of each changed node.
    const graph = new Graph();
    const contexts = this.new();

    for (const [id] of update) {
      const node = update.value(id);

      if (node instanceof Context) {
        contexts.merge({ [id]: node });
      } else {
        const context = new Context(this, { uid: id });
        context.merge(node);
        contexts.merge({ [id]: context });
      }

      // If the node exists in the graph...
      const current = this.value(id);

      if (current) {
        const { uid } = current.meta();

        // Merge it with the update.
        graph.merge({ [uid]: current });
      }
    }

    graph.merge(update);

    const config = await pipeline.before.write(this[settings], {
      offline: this[settings].network.filter(isOffline),
      ...options,
      update,
      graph,
    });

    // Merge in the update.
    const deltas = this.merge(contexts);

    const write = {
      ...config,
      ...deltas,
    };

    // Persist.
    const writes = config.storage.map((store) => store.write(write));

    if (this.router) {
      await this.router.push(write);
    }

    // Wait for writes to finish.
    await Promise.all(writes);

    return await pipeline.after.write(this[settings], write);
  }

  /**
   * Merges a node into the graph.
   * @param  {String} uid - The unique ID of the node.
   * @param  {Object} value - The fields to add/update.
   * @param  {Object} [options] - Override default behavior.
   * @return {Context} - Resolves to the context written.
   */
  async write (uid, value, options = {}) {

    // Turn the object into a database context.
    const context = new Context(this, { uid });
    const current = this.value(uid);

    // Ensure object updates increment state.
    if (current && value) {
      for (const field in value) {
        if (value.hasOwnProperty(field)) {
          context.merge({ [field]: current.value(field) });
          context.meta(field).state = current.state(field);
        }
      }
    }

    context.merge(value);

    const update = Graph.source({ [uid]: context });

    await this.commit(update, options);

    return this.value(uid);
  }

  /**
   * Read a context from the database.
   * @param  {String} key - The node's unique ID.
   * @param  {Object} [options] - Plugin-level options.
   * @param  {ConnectionGroup|null} [options.network]
   * Override the group of network connections.
   * @param  {Object|null} [options.storage] - Storage plugins to use.
   * @param  {Boolean} [options.force] - Ignore cache and force read.
   * @return {Context|null} - Resolves to the node.
   */
  async read (key, options = {}) {
    const config = await pipeline.before.read.node(this[settings], {
      offline: this[settings].network.filter(isOffline),
      ...options,
      key,
    });

    let node = this.value(config.key);

    // Not cached.
    if (node === null || config.force) {
      node = await readFromPlugins(this, config);

      // If data was found, cache the response.
      if (node) {
        this.merge({ [config.key]: node });
      }
    }

    // After-read hooks.
    const result = await pipeline.after.read.node(this[settings], {
      context: this.value(config.key),
      ...config,
    });

    return result.context;
  }

  /**
   * description
   * @param  {Mixed} query - query that is passed to the engine
   * @param  {Object} options - options for executing the query
   * @return {undefined}
   */
  query (query, options) {
    const config = this[settings];
    const engines = config.engines;
    const engine = options.engine;

    // Make sure the engine exists
    if (!engines.hasOwnProperty(engine)) {
      throw new Error('No engine is defined in options');
    }

    const queryEngine = engines[engine];
    return queryEngine.executeQuery(query, this);
  }
}

/**
 * Creates a new instance of `Database` with or without `new`.
 * @param  {...Object} configs - Plugins and configurations for the database.
 * @return {Database} - A new database instance.
 */
function database (...configs) {
  const config = merge(configs);

  return new Database(config);
}

database.configuration = settings;

export default database;
