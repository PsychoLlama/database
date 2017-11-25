// @flow
import createConfig from '../create-config';

describe('Config formatter', () => {
  it('returns an object given nothing', () => {
    const result = createConfig();

    expect(result).toEqual(expect.any(Object));
  });

  it('provides default options', () => {
    const config = createConfig();

    expect(config).toEqual({
      storage: null,
      hooks: [],
      network: {
        connections: [],
        router: null,
      },
    });
  });

  it('uses the storage engine if provided', () => {
    const storage = {
      write: async () => ({}),
      read: async () => ({}),
    };

    const config = createConfig({ storage });

    expect(config.storage).toBe(storage);
  });

  it('adds all given hooks', () => {
    const hook1 = read => read;
    const hook2 = read => read;
    const hooks = [hook1, hook2];

    const config = createConfig({ hooks });

    expect(config.hooks).toEqual(hooks);
  });

  it('uses the given network settings', () => {
    const network = {
      connections: [],
      router: {
        push: async () => {},
        pull: async () => {},
      },
    };

    const config = createConfig({ network });

    expect(config.network).toEqual(network);
  });
});
