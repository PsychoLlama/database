// @flow
import { Add, Remove, TypeChange, Migration } from '../migrations';
import Composite from '../Composite';
import Primitive from '../Primitive';

const string = new Primitive('string', {
  isValid: value => typeof value === 'string',
  coerce: String,
});

const number = new Primitive('number', {
  isValid: value => typeof value === 'number',
  coerce: Number,
});

describe('Migration', () => {
  const createType = () =>
    new Composite('Player', {
      initialFieldSet: {
        firstName: string,
        gamertag: string,
        lastName: string,
        score: number,
      },
    });

  describe('Add', () => {
    it('creates a migration', () => {
      const migration = new Add('name', string);

      expect(migration).toEqual(expect.any(Migration));
      expect(migration.name).toBe('ADD');
    });

    it('updates the field set', () => {
      const type = createType();
      const migration = new Add('joined', string);
      const result = migration.migrateType(type);

      expect(result).toEqual({
        defaultType: type.defaultType,
        definition: {
          ...type.definition,
          joined: string,
        },
      });
    });

    it('updates the data set', () => {
      const migration = new Add('joined', string);
      const result = migration.migrateData({
        gamertag: 'z33k',
      });

      expect(result).toEqual({ gamertag: 'z33k', joined: undefined });
    });

    it('throws if the field exists', () => {
      const migration = new Add('gamertag', string);
      const type = createType();
      const fail = () => migration.migrateType(type);

      expect(fail).toThrow(/exists?/i);
    });
  });

  describe('Remove', () => {
    it('creates a migration', () => {
      const migration = new Remove('gamertag');

      expect(migration).toEqual(expect.any(Migration));
      expect(migration.name).toBe('REMOVE');
    });

    it('drops the field from the type', () => {
      const migration = new Remove('gamertag');
      const type = createType();
      const result = migration.migrateType(type);

      const expected = { ...type.definition };
      delete expected.gamertag;

      expect(result.definition).toEqual(expected);
      expect(result.defaultType).toBe(type.defaultType);
    });

    it('drops the field from data', () => {
      const migration = new Remove('lastName');
      const result = migration.migrateData({
        firstName: 'Bob',
        lastName: 'Bobinson',
      });

      expect(result).toEqual({ firstName: 'Bob' });
    });

    it('throws if the field does not exist', () => {
      const migration = new Remove('wat');
      const type = createType();
      const fail = () => migration.migrateType(type);

      expect(fail).toThrow(/exists?/i);
    });
  });

  describe('TypeChange', () => {
    it('creates a migration', () => {
      const migration = new TypeChange('gamertag', number);

      expect(migration).toEqual(expect.any(Migration));
      expect(migration.name).toBe('CHANGE_TYPE');
    });

    it('throws if the field does not exist', () => {
      const migration = new TypeChange('bacon', string);
      const type = createType();
      const fail = () => migration.migrateType(type);

      expect(fail).toThrow(/exists?/i);
    });

    it('updates the type', () => {
      const migration = new TypeChange('score', string);
      const type = createType();
      const result = migration.migrateType(type);

      expect(result).toEqual({
        defaultType: type.defaultType,
        definition: {
          ...type.definition,
          score: string,
        },
      });
    });

    // Requires primitive coercion interface.
    it('updates data');
  });
});
