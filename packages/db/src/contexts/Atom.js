// @flow
import type Primitive, { JsonPrimitive } from '@mytosis/types/dist/Primitive';
import type { Composite, Derivation } from '@mytosis/types';
import type { Dehydrated } from '@mytosis/crdts/dist/Atom';
import { Atom } from '@mytosis/crdts';

import type DatabaseContext from '../database-context';

type Definition = {
  context: DatabaseContext,
  type: Composite,
  id: string,
};

type DataImport = Definition & {
  data: Dehydrated,
};

type FieldMetadata = {
  type: Primitive | Derivation,
  value: JsonPrimitive,
};

/** Developer-facing atom CRDT interface. */
export default class AtomContext {
  _context: DatabaseContext;
  _atom: Atom;

  type: Composite;
  id: string;

  /**
   * Imports data into a new atom.
   * @param  {Object} definition - Describes the new atom.
   * @return {Atom} - New atom with the initial state.
   */
  static import({ id, type, data, context }: DataImport) {
    const atom = new AtomContext({ id, type, context });
    const crdt = Atom.import(data);
    Object.defineProperty(atom, '_atom', { value: crdt });

    return atom;
  }

  /**
   * Creates a new atom.
   * @param  {Object} definition - Initial atom state.
   * @param  {String} definition.id - A unique ID.
   * @param  {Composite} definition.type - Composite describing atom structure.
   * @return {Atom} - Shiny new atom.
   */
  static new({ id, type, context }: Definition) {
    const atom = new AtomContext({ id, type, context });

    Object.defineProperty(atom, '_atom', {
      value: new Atom(),
    });

    return atom;
  }

  /**
   * Instantiates _most_ of the atom. Only used by static methods as a starting point.
   * @param  {Object} definition - Describes the atom.
   * @private
   */
  constructor({ id, type, context }: Definition) {
    Object.defineProperty(this, '_context', { value: context });
    this.type = type;
    this.id = id;
  }

  /**
   * Gets metadata about a field, including its raw value.
   * @param  {String} field - Field to describe.
   * @return {Object} - Metadata.
   */
  getFieldMetadata(field: string): FieldMetadata {
    const { definition, defaultType } = this.type;
    const hasPreciseDefinition = definition.hasOwnProperty(field);
    const type = hasPreciseDefinition ? definition[field] : defaultType;
    const value = this._atom.read(field);

    if (!type) {
      throw new Error(
        `Type ${this.type.name} doesn't define field "${field}".`,
      );
    }

    return { value, type };
  }
}