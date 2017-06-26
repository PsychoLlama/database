/* eslint-env node */
import { ConnectionGroup } from 'mytosis';
import SocketServer from '../server';
import { Server } from 'uws';
import Emitter from 'events';

jest.mock('uws', () => ({
  Server: jest.fn(),
}));

describe('Mytosis websocket server', () => {
  let server, wss;

  beforeEach(() => {
    wss = new Emitter();
    Server.mockReset();
    Server.mockImplementation(function () {
      return wss;
    });

    server = new SocketServer({ port: 8080 });
  });

  it('creates a connection group', () => {
    expect(server).toEqual(expect.any(ConnectionGroup));
  });

  it('creates a new socket server', () => {
    expect(Server).toHaveBeenCalled();
  });

  it('passes uses the config given', () => {
    server = new SocketServer({ port: 3000 });

    expect(Server).toHaveBeenCalledWith({ port: 3000 });
    expect(Server.mock.instances.length).toBe(2);
  });

  it('throws if config is ommitted', () => {
    const fail = () => new SocketServer();

    expect(fail).toThrow(/config/);
  });

  it('registers clients when they connect', () => {
    const spy = jest.fn();
    server.on('add', spy);

    const socket = new Emitter();
    wss.emit('connection', socket);

    expect(spy).toHaveBeenCalledWith(expect.any(Object));
  });

  it('removes clients when they disconnect', () => {
    const spy = jest.fn();
    server.on('remove', spy);

    const socket = new Emitter();
    wss.emit('connection', socket);
    socket.emit('close');

    expect(spy).toHaveBeenCalledWith(expect.any(Object));
  });

  it('attaches a unique ID to each socket', () => {
    const socket = new Emitter();
    const spy = jest.fn();

    server.on('add', spy);
    wss.emit('connection', socket);

    const connection = spy.mock.calls[0][0];

    expect(connection.id).toEqual(expect.any(String));
  });

  it('stringifies objects before sending them', () => {
    const data = { activate: 'bacon' };
    const socket = new Emitter();
    socket.send = jest.fn();

    wss.emit('connection', socket);
    server.send(data);

    expect(socket.send).toHaveBeenCalledWith(JSON.stringify(data));
  });

  it('does not attempt to stringify buffers', () => {
    const data = Buffer.from('Hello binary!');
    const socket = new Emitter();
    socket.send = jest.fn();

    wss.emit('connection', socket);
    server.send(data);

    expect(socket.send).toHaveBeenCalledWith(data);
  });

  it('exposes messages', () => {
    const data = { hasMeaning: false };
    const string = JSON.stringify(data);
    const socket = new Emitter();

    wss.emit('connection', socket);

    const spy = jest.fn();

    server.messages.forEach(spy);
    socket.emit('message', string);

    expect(spy).toHaveBeenCalledWith(data, expect.anything());
  });

  it('turns array buffers into buffer objects', () => {

    // The .buffer property is the underlying `ArrayBuffer`.
    const { buffer } = new Uint8Array([1, 2, 3]);
    const socket = new Emitter();
    const spy = jest.fn();

    wss.emit('connection', socket);
    server.messages.forEach(spy);
    socket.emit('message', buffer);

    expect(spy).toHaveBeenCalled();

    const [bufferActual] = spy.mock.calls[0];
    expect([...bufferActual]).toEqual([1, 2, 3]);
  });

  it('unsubscribes when the stream is unobserved', () => {
    const socket = new Emitter();
    socket.removeListener = jest.fn();

    wss.emit('connection', socket);
    server.messages.forEach(jest.fn()).dispose();

    expect(socket.removeListener).toHaveBeenCalledWith(
      'message',
      expect.any(Function)
    );
  });

  it('adds a type to the socket', () => {
    const socket = new Emitter();
    wss.emit('connection', socket);

    const websockets = server.type('websocket');

    expect([...websockets].length).toBe(1);
  });
});
