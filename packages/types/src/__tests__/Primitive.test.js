import Primitive from '../Primitive';

describe('Primitive', () => {
  it('is a constructor', () => {
    expect(Primitive).toEqual(expect.any(Function));
  });

  it('throws if the name contains non-word characters', () => {
    const def = { isValid: () => false, coerce: String };

    expect(() => new Primitive('', def)).toThrow(/name/i);
    expect(() => new Primitive('   ', def)).toThrow(/name/i);
    expect(() => new Primitive(' name', def)).toThrow(/name/i);
    expect(() => new Primitive('Uppercased', def)).toThrow(/name/i);
    expect(() => new Primitive('8-leading-number', def)).toThrow(/name/i);
    expect(() => new Primitive('sym&ols', def)).toThrow(/name/i);
    expect(() => new Primitive('camelCased', def)).toThrow();

    expect(() => new Primitive('time', def)).not.toThrow();
    expect(() => new Primitive('name-hyphen', def)).not.toThrow();
    expect(() => new Primitive('with-numb3r5', def)).not.toThrow();
  });

  it('consults the validator', () => {
    const isValid = jest.fn(() => false);
    const type = new Primitive('string', { isValid, coerce: String });
    const result = type.isValid(5);

    expect(isValid).toHaveBeenCalledWith(5);
    expect(result).toBe(false);
  });

  it('always returns false when given `undefined`', () => {
    const isValid = jest.fn(() => true);
    const type = new Primitive('any', { isValid, coerce: String });

    expect(type.isValid(undefined)).toBe(false);
    expect(isValid).not.toHaveBeenCalled();
  });

  it('can coerce values', () => {
    const type = new Primitive('string', {
      isValid: value => typeof value === 'string',
      coerce: String,
    });

    expect(type.coerce(5)).toBe('5');
    expect(type.coerce('data')).toBe('data');
  });
});
