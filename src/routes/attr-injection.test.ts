import test from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { isValidAttributeKey } from '../validation/log-validator';
import { queryRoutes } from './query';
import { aggregateRoutes } from './aggregate';

test('Attribute Key Validator Unit Tests', async (t) => {
  await t.test('valid attribute key: attr.userId', () => {
    assert.strictEqual(isValidAttributeKey('userId'), true);
  });

  await t.test('valid key containing _ (user_id)', () => {
    assert.strictEqual(isValidAttributeKey('user_id'), true);
  });

  await t.test('valid key containing - (user-id)', () => {
    assert.strictEqual(isValidAttributeKey('user-id'), true);
  });

  await t.test('valid key containing . (user.id)', () => {
    assert.strictEqual(isValidAttributeKey('user.id'), true);
  });

  await t.test("invalid key containing single quote (')", () => {
    assert.strictEqual(isValidAttributeKey("foo'bar"), false);
  });

  await t.test('invalid key containing double quote (")', () => {
    assert.strictEqual(isValidAttributeKey('foo"bar'), false);
  });

  await t.test('invalid key containing semicolon (;)', () => {
    assert.strictEqual(isValidAttributeKey('foo;bar'), false);
  });

  await t.test("invalid key containing SQL syntax (foo' OR 1=1 --)", () => {
    assert.strictEqual(isValidAttributeKey("foo' OR 1=1 --"), false);
  });
});

test('HTTP Endpoint Integration Tests - attr.<key> SQL Injection Prevention', async (t) => {
  let executedSql = '';
  let executedValues: any[] = [];

  const mockPool: any = {
    query: async (sql: string, params: any[]) => {
      executedSql = sql;
      executedValues = params;
      return { rows: [] };
    }
  };

  const app = Fastify();
  await app.register(queryRoutes(mockPool));
  await app.register(aggregateRoutes(mockPool));

  await t.test('GET /logs - valid attr.userId returns 200 and uses parameterized comparison value', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.userId=usr_123'
    });
    assert.strictEqual(res.statusCode, 200);
    assert.ok(executedSql.includes("attributes->>'userId' = $1"));
    assert.deepStrictEqual(executedValues, ['usr_123', 101]); // default limit is 100, +1 probe row
  });

  await t.test('GET /logs - valid attr.user_id returns 200', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.user_id=usr_456'
    });
    assert.strictEqual(res.statusCode, 200);
    assert.ok(executedSql.includes("attributes->>'user_id' = $1"));
  });

  await t.test('GET /logs - valid attr.user-id returns 200', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.user-id=usr_789'
    });
    assert.strictEqual(res.statusCode, 200);
    assert.ok(executedSql.includes("attributes->>'user-id' = $1"));
  });

  await t.test('GET /logs - valid attr.user.id returns 200', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.user.id=usr_999'
    });
    assert.strictEqual(res.statusCode, 200);
    assert.ok(executedSql.includes("attributes->>'user.id' = $1"));
  });

  await t.test("GET /logs - invalid key containing single quote (attr.foo' OR 1=1 --) returns 400", async () => {
    const res = await app.inject({
      method: 'GET',
      url: "/logs?attr.foo' OR 1=1 --=bar"
    });
    assert.strictEqual(res.statusCode, 400);
    const body = JSON.parse(res.body);
    assert.ok(body.error.includes('Invalid attribute key format'));
  });

  await t.test('GET /logs - invalid key containing double quote (attr.foo"bar) returns 400', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.foo"bar=val'
    });
    assert.strictEqual(res.statusCode, 400);
  });

  await t.test('GET /logs - invalid key containing semicolon (attr.foo;DROP TABLE logs) returns 400', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/logs?attr.foo;DROP=val'
    });
    assert.strictEqual(res.statusCode, 400);
  });

  await t.test("GET /logs/aggregate - invalid key containing single quote (attr.foo' OR 1=1 --) returns 400", async () => {
    const res = await app.inject({
      method: 'GET',
      url: "/logs/aggregate?since=2026-08-01T00:00:00Z&until=2026-08-02T00:00:00Z&bucket=1m&attr.foo' OR 1=1 --=bar"
    });
    assert.strictEqual(res.statusCode, 400);
    const body = JSON.parse(res.body);
    assert.ok(body.error.includes('Invalid attribute key format'));
  });
});
