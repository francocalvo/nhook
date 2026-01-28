# Workflows

This document describes the workflows shipped with NHook: what triggers them, what they do, and what you need to configure in Notion.

## How workflow selection works

NHook selects a workflow for each webhook request:

1. If the request includes `X-Calvo-Workflow`, NHook runs the workflow whose `name` matches that header.
2. Otherwise, NHook finds the first registered workflow whose `matches()` returns `True`.

In production, prefer sending `X-Calvo-Workflow` so selection is deterministic.

## Common expectations

### Authentication

All webhook requests must include:

```
X-Calvo-Key: <WEBHOOK_SECRET_KEY>
```

### Payload shape

NHook expects Notion's automation webhook shape:

- Page ID at `data.id`
- Properties under `data.properties`

Date properties are parsed from the nested Notion structure:

```json
{
  "type": "date",
  "date": {"start": "2026-03-14", "end": null, "time_zone": null}
}
```

Datetime values (e.g. `2026-03-14T10:00:00Z`) are normalized to a date at parse time.

## Workflows

### `gastos-cronograma`

**Purpose**: Sync the `Cronograma` relation on a Gastos page when its `Date` changes.

**Trigger**:

- Recommended: set `X-Calvo-Workflow: gastos-cronograma`
- Property used: `Date` (case-insensitive)

**Behavior**:

- If `Date` is empty: clears the `Cronograma` relation.
- If `Date` is a single date: links all Cronograma entries matching that date.
- If `Date` is a date range: links all Cronograma entries in that range.

**Notion requirements**:

- Gastos database has a date property named `Date`.
- Gastos database has a relation property named `Cronograma`.
- Cronograma database has a date property that `find_cronograma_by_dates()` queries (see `NotionClient`).

### `pasajes-cronograma`

**Purpose**: Sync the `Cronograma` relation on a Pasajes page when its departure date changes.

**Trigger**:

- Recommended: set `X-Calvo-Workflow: pasajes-cronograma`
- Property used: `Departure` (case-insensitive)

**Behavior**:

- If `Departure` is empty: clears the `Cronograma` relation.
- If `Departure` is set: links all Cronograma entries matching that date.

### `atracciones-cronograma`

**Purpose**: Sync the `Cronograma` relation on an Atracciones page when its `Fecha` changes.

**Trigger**:

- Recommended: set `X-Calvo-Workflow: atracciones-cronograma`
- Property used: `Fecha` (case-insensitive)

**Behavior**:

- If `Fecha` is empty: clears the `Cronograma` relation.
- If `Fecha` is set: links all Cronograma entries matching that date.
- If no Cronograma entries match: clears the relation and returns an empty result.

### `gastos-sync`

**Purpose**: Mirror Gastos pages into local SQLite (create/update/delete) and log failures.

**Trigger**:

- Required: `X-Calvo-Workflow: gastos-sync`

**Behavior (high level)**:

- Detects operation:
  - `delete` if `archived=true`, `in_trash=true`, or `properties` is empty
  - otherwise `create` vs `update` based on whether the `page_id` exists locally
- Parses Notion properties and writes to SQLite.

See **[Gastos Feature](./gastos.md)** for schema and reload API.

## Adding a new workflow

See **[Extending](./extending.md)**.
