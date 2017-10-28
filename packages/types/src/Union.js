// @flow
import assert from 'minimalistic-assert';

import Derivation from './Derivation';
import type Literal from './Literal';
import Composite from './Composite';
import Primitive from './Primitive';

type TypeList = Array<Primitive | Literal | Derivation>;
type Coercible = Primitive | Derivation;

/**
 * Represents a list of possible primitives or
 * literal values. Incompatible with composites and other unions.
 */
export default class Union extends Primitive {
  /**
   * @param  {Primitive} coercion - The type to use in a coercion.
   * @param  {Array} types - Primitive or literal values.
   */
  constructor(coercion: Coercible, types: TypeList) {
    assert(types.length, 'List of union values is empty.');
    const typeSet = new Set(types);

    // Flow doesn't catch these. I'm probably making a n00b mistake.
    types.forEach(type => {
      const notComposite = !(type instanceof Composite);
      const notUnion = !(type instanceof Union);

      assert(notUnion, `Unions cannot contain other unions.`);
      assert(
        notComposite,
        `Unions cannot contain composite types (given ${type.name}).`,
      );

      // Ensure types can always be inferred by value.
      if (type instanceof Derivation) {
        assert(
          !typeSet.has(type.subtype),
          `Union contains ambiguous types (${type.name} & ${type.subtype
            .name}).`,
        );
      }
    });

    // Resolve the correct coercion interface even through derived types.
    const ambassador =
      coercion instanceof Derivation ? coercion.subtype : coercion;

    super('union', {
      isValid: value => types.some(type => type.isValid(value)),
      coerce: value => ambassador.coerce(value),
    });
  }
}
