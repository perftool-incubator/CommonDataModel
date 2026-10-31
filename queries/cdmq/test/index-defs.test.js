const test = require('node:test');
const assert = require('node:assert/strict');

const cdm = require('../cdm');

test('clpeak metric breakout fields are registered in index definitions', () => {
  const fields = ['backend', 'device_index', 'test', 'unit', 'variant'];
  const versions = ['v8dev', 'v9dev', 'v10dev'];

  for (const version of versions) {
    const mappingPaths = cdm.getMappingFieldPaths(cdm.indexDefs[version].metric_desc.mappings.properties);

    for (const field of fields) {
      assert.equal(
        mappingPaths.has(`metric_desc.names.${field}`),
        true,
        `${version} must define metric_desc.names.${field}`
      );
    }
  }
});
