import { ValidatedLog } from '../worker/log-worker';

export interface ValidationResult {
  valid: boolean;
  log?: ValidatedLog;
  reason?: string;
}

const VALID_LEVELS = new Set(['debug', 'info', 'warn', 'error']);
const FIVE_MINUTES_MS = 5 * 60 * 1000;
const ATTR_KEY_REGEX = /^[a-zA-Z0-9_.-]+$/;

export function isValidAttributeKey(key: string): boolean {
  return typeof key === 'string' && key.length > 0 && ATTR_KEY_REGEX.test(key);
}

export function validateLogEntry(entry: any, nowMs: number): ValidationResult {
  if (!entry || typeof entry !== 'object') {
    return { valid: false, reason: 'Entry must be an object' };
  }

  // Timestamp — Date.parse returns a number, no object allocation
  if (!entry.timestamp || typeof entry.timestamp !== 'string') {
    return { valid: false, reason: 'Missing or invalid timestamp' };
  }
  const ts = Date.parse(entry.timestamp);
  if (ts !== ts) { // NaN !== NaN (faster than isNaN)
    return { valid: false, reason: 'Timestamp must be a valid ISO 8601 string' };
  }
  if (ts > nowMs + FIVE_MINUTES_MS) {
    return { valid: false, reason: 'Timestamp cannot be more than 5 minutes in the future' };
  }

  // Level — Set.has is O(1)
  if (!entry.level || !VALID_LEVELS.has(entry.level)) {
    return { valid: false, reason: `Invalid level: '${entry.level}'` };
  }

  // Service
  if (typeof entry.service !== 'string' || entry.service.length === 0) {
    return { valid: false, reason: 'Service must be a non-empty string' };
  }

  // Message
  if (typeof entry.message !== 'string' || entry.message.length === 0) {
    return { valid: false, reason: 'Message must be a non-empty string' };
  }

  // Attributes — for...in avoids Object.entries() intermediate array
  if (entry.attributes !== undefined && entry.attributes !== null) {
    if (typeof entry.attributes !== 'object' || Array.isArray(entry.attributes)) {
      return { valid: false, reason: 'Attributes must be a flat object' };
    }
    for (const key in entry.attributes) {
      const valType = typeof entry.attributes[key];
      if (valType !== 'string' && valType !== 'number' && valType !== 'boolean') {
        return { valid: false, reason: `Attribute '${key}' has invalid type '${valType}'` };
      }
    }
  }

  // Zero-copy: return reference to original parsed object
  return {
    valid: true,
    log: entry as ValidatedLog
  };
}