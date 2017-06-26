/* eslint-env node */
import { ConnectionGroup, Stream } from 'mytosis';
import { Server } from 'uws';
import uuid from 'uuid/v4';

import { asserter } from './utils';

const assert = asserter('Mytosis SocketServer');

/**
 * Interface over a websocket connection.
 * @class
 */
class Connection {

  /**
   * @param  {Object} socket - WebSocket instance.
   */
  constructor (socket) {
    this.id = uuid();
    this.type = 'websocket';

    this._socket = socket;

    this.messages = new Stream((push) => {
      const parseMessage = (message) => {
        const data = typeof message === 'string'
          ? JSON.parse(message)
          : Buffer.from(message);

        push(data);
      };

      socket.on('message', parseMessage);

      // Quit if nothing is watching the stream.
      return () => socket.removeListener('message', parseMessage);
    });
  }

  /**
   * Sends a message (without acknowledgement).
   * @param  {Object|Buffer} message - JSON or a binary buffer.
   * @return {undefined}
   */
  send (message) {

    // WebSockets support binary messages.
    const payload = Buffer.isBuffer(message)
      ? message
      : JSON.stringify(message);

    this._socket.send(payload);
  }
}

/**
 * Creates a new socket server.
 * @class
 */
export default class SocketServer extends ConnectionGroup {

  /**
   * @param  {Object} config - Server options.
   */
  constructor (config) {
    assert(config, `Expected config, got "${config}"`);

    super();

    const server = new Server(config);
    server.on('connection', (socket) => {
      const connection = new Connection(socket);

      this.add(connection);

      socket.on('close', () => this.remove(connection));
    });
  }
}
