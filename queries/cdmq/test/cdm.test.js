const test = require('node:test');
const assert = require('node:assert/strict');
const cdm = require('../cdm.js');


test('set(ish) difference (subtractTwoArrays)', (t) => {
  a0 = [1, 1, 2]
  a1 = [2, 2, 3]

  assert.deepEqual(
    cdm.subtractTwoArrays(a0, a1),
    [1, 1]
  )
  assert.deepEqual(
    cdm.subtractTwoArrays(a1, a0),
    [3]
  )
})


test('set(ish) intersection (intersectTwoArrays)', (t) => {
  a0 = [1, 1, 2]
  a1 = [2, 2, 3]

  assert.deepEqual(
    cdm.intersectTwoArrays(a0, a1),
    [2]
  )
  assert.deepEqual(
    cdm.intersectTwoArrays(a1, a0),
    [2, 2]
  )
})


test('intersect all arrays', (t) => {
  a0 = [1, 1, 2]
  a1 = [2, 2, 3]
  a2 = [4, 4, 2]
  a = [a0, a1, a2]
  assert.deepEqual(
    cdm.intersectAllArrays(a),
    [2]
  )
})