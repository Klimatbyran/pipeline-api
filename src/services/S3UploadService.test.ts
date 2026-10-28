import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { encodeObjectMetadataValue } from './S3UploadService.js';

describe('encodeObjectMetadataValue', () => {
  it('leaves ASCII filenames unchanged aside from spaces and reserved chars', () => {
    assert.equal(encodeObjectMetadataValue('Climate_Report_2024.pdf'), 'Climate_Report_2024.pdf');
  });

  it('encodes Swedish and other non-ASCII characters for S3/GCS metadata headers', () => {
    assert.equal(
      encodeObjectMetadataValue('hållbarhetsrapport-År-2024.pdf'),
      'h%C3%A5llbarhetsrapport-%C3%85r-2024.pdf'
    );
    assert.equal(decodeURIComponent(encodeObjectMetadataValue('hållbarhetsrapport-År-2024.pdf')), 'hållbarhetsrapport-År-2024.pdf');
  });

  it('encodes spaces so metadata stays header-safe', () => {
    assert.equal(encodeObjectMetadataValue('my report.pdf'), 'my%20report.pdf');
  });
});
