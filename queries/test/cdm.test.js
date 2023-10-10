const { describe, expect, test } = require('@jest/globals');

const cdm = require('../cdmq/cdm.js');

describe('set(ish)-array behavior', () => {
  test('set(ish) difference (subtractTwoArrays)', () => {
    a0 = [1, 1, 2];
    a1 = [2, 2, 3];

    expect(cdm.subtractTwoArrays(a0, a1)).toStrictEqual([1, 1]);
    expect(cdm.subtractTwoArrays(a1, a0)).toStrictEqual([3]);
  });

  test('set(ish) intersection (intersectTwoArrays)', () => {
    a0 = [1, 1, 2];
    a1 = [2, 2, 3];

    expect(cdm.intersectTwoArrays(a0, a1)).toStrictEqual([2]);
    expect(cdm.intersectTwoArrays(a1, a0)).toStrictEqual([2, 2]);
  });

  test('intersect all arrays', () => {
    a0 = [1, 1, 2];
    a1 = [2, 2, 3];
    a2 = [4, 4, 2];
    a = [a0, a1, a2];
    expect(cdm.intersectAllArrays(a)).toStrictEqual([2]);
  });
});

// const { t } = require('tap');

// t.test('set(ish) difference (subtractTwoArrays)', t => {
//     a0 = [1, 1, 2];
//     a1 = [2, 2, 3];

//     t.matchOnlyStrict(cdm.subtractTwoArrays(a0, a1), [1, 1]);
//     t.matchOnlyStrict(cdm.subtractTwoArrays(a1, a0), [3]);
//     t.end();
// });

// t.test('set(ish) intersection (intersectTwoArrays)', t => {
//     a0 = [1, 1, 2];
//     a1 = [2, 2, 3];

//     t.matchOnlyStrict(cdm.intersectTwoArrays(a0, a1), [2]);
//     t.matchOnlyStrict(cdm.intersectTwoArrays(a1, a0), [2, 2]);
//     t.end();
// });

// t.test('intersect all arrays', t => {
//     a0 = [1, 1, 2];
//     a1 = [2, 2, 3];
//     a2 = [4, 4, 2];
//     a = [a0, a1, a2];
//     t.matchOnlyStrict(cdm.intersectAllArrays(a), [2]);
//     t.end()
// });



