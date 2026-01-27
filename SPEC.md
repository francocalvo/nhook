# NHook project

## Current state

In my project 'Italia' I have two important things I want to link:

'Cronograma' database:
https://www.notion.so/calvo/2f5f6e7f05728052ac90e569e8b1e944?v=2f5f6e7f057280229e18000c2cfbd9b1
'Gastos' database:
https://www.notion.so/calvo/2e2f6e7f0572800c962ae8c9bf6cca51?v=2e2f6e7f057280989089000c3a90edd6

The 'Gastos' database has two important columns to consider:

- 'Date' column, of format date (which might be a range!).
- 'Cronograma' column, of format 'relationship', which relates to Cronograma
  entries.

The 'Cronograma' database has two important columns:

- The 'Día' which is the date in 'yyyy-mm-dd' format. It's a string and the
  'name' of the entry.
- The 'Date' column, which is of date format.

Examples: Cronograma entry:

- https://www.notion.so/calvo/2f5f6e7f05728052ac90e569e8b1e944?v=2f5f6e7f057280229e18000c2cfbd9b1&p=2f5f6e7f05728077903ada5d89f654f4&pm=s

Gastos entry:

- https://www.notion.so/calvo/2e2f6e7f0572800c962ae8c9bf6cca51?v=2e2f6e7f057280989089000c3a90edd6&p=2f5f6e7f057280849493f7f8b50744c7&pm=s
- https://www.notion.so/calvo/2e2f6e7f0572800c962ae8c9bf6cca51?v=2e2f6e7f057280989089000c3a90edd6&p=2f5f6e7f057280d28972fc5637d7bb3e&pm=s

## Feature

I need to implement a way to keep in sync the relationship 'Cronograma' in the
Gastos database.

This should auto-update when I change the 'Date' property.

Examples:

- if I change the '2026-03-14' to '2026-03-15' in the Brunelleschi Pass example
  I shared, it should clear the current relationships in 'Cronograma' property
  and fill the new ones that match that.
- If I fill the 'Date' in the 'Alojamiento en Florencia' entry, it should clear
  the 'Cronograma' relationship, and fill with all the dates in the range from
  the Cronograma.

## Implementation

### Notion side:

- The 'Cronograma' database is already filled. All dates will match.
- In Notion, I'll enable a automation which will send a webhook on each 'Date'
  property update. This will be our way to know that we need to do something.
- I'll add a header: 'X-Calvo-Key', which we need to match. If the webhook
  doesn't include the header, we IGNORE.

### Server side

- We need to implement a server which will handle the webhook. This should be
  done using FastAPI in Python.
- We need to assume that this will be the first use case of this type, so create
  a structure that will allow to have more automation workflows cleanly.
- We will have two consider two modes: I could run this from my local machine,
  but the long term is that this will run inside a OCI image and exposed using
  Nginx.
