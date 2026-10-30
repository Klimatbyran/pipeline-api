import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  OBJECT_METADATA_VALUE_MAX_CHARS,
  encodeObjectMetadataValue,
} from './S3UploadService.js';

describe('encodeObjectMetadataValue', () => {
  it('leaves plain ASCII filenames unchanged', () => {
    assert.equal(encodeObjectMetadataValue('Climate_Report_2024.pdf'), 'Climate_Report_2024.pdf');
  });

  it('encodes Swedish and other non-ASCII characters for S3/GCS metadata headers', () => {
    const name = 'hållbarhetsrapport-År-2024.pdf';
    assert.equal(
      encodeObjectMetadataValue(name),
      'h%C3%A5llbarhetsrapport-%C3%85r-2024.pdf'
    );
    assert.equal(decodeURIComponent(encodeObjectMetadataValue(name)), name);
  });

  it('encodes spaces so metadata stays header-safe', () => {
    assert.equal(encodeObjectMetadataValue('my report.pdf'), 'my%20report.pdf');
  });

  it('encodes lone surrogates without throwing', () => {
    const lone = 'report-\uD800.pdf';
    assert.doesNotThrow(() => encodeObjectMetadataValue(lone));
    assert.match(encodeObjectMetadataValue(lone), /^[\x20-\x7e]+$/);
  });

  it('truncates oversized values to a valid percent-encoded prefix', () => {
    const longAscii = `${'a'.repeat(OBJECT_METADATA_VALUE_MAX_CHARS + 50)}.pdf`;
    const encoded = encodeObjectMetadataValue(longAscii);
    assert.ok(encoded.length <= OBJECT_METADATA_VALUE_MAX_CHARS);
    assert.equal(encoded, 'a'.repeat(OBJECT_METADATA_VALUE_MAX_CHARS));

    // Non-ASCII expands ~6x; result must stay ASCII, under the cap, and decodable.
    const longSwedish = `${'å'.repeat(500)}.pdf`;
    const swedishEncoded = encodeObjectMetadataValue(longSwedish);
    assert.ok(swedishEncoded.length <= OBJECT_METADATA_VALUE_MAX_CHARS);
    assert.match(swedishEncoded, /^[\x20-\x7e]*$/);
    assert.equal(decodeURIComponent(swedishEncoded), 'å'.repeat(Math.floor(OBJECT_METADATA_VALUE_MAX_CHARS / 6)));
  });
});
