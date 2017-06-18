import WebSocket from 'reconnecting-websocket';
import { Stream } from 'mytosis';

import { asserter } from './utils';

const assert = asserter('Mytosis WebSocket');

/**
 * Opens a websocket connection to the given URL.
 * @class
 */
export default class Socket {

  /**
   * @param  {String} url - WebSocket protocol formatted url.
   * @param  {Object} [config] - WebSocket protocol formatted url.
   */
  constructor (url, config = {}) {
    assert(typeof url === 'string', `Got URL "${url}" (expected string)`);
    const { protocols, ...options } = config;

    this.id = `mytosis-websocket:${url}`;
    this.type = 'websocket';
    this.offline = true;

    const socket = new WebSocket(url, protocols, options);

    // Handle incoming messages.
    this.messages = new Stream((push) => {
      const parseMessages = ({ data }) => {

        // Support binary (Blob/ArrayBuffer) payloads.
        const payload = typeof data === 'string'
          ? JSON.parse(data)
          : data;

        push(payload);
      };

      socket.addEventListener('message', parseMessages);
    });

    // Adjust the `offline` flag based on the channel state.
    socket.addEventListener('open', () => {
      this.offline = false;
    });

    socket.addEventListener('close', () => {
      this.offline = true;
    });
  }
}
