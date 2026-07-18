# Two RabbitMQ API styles

This repository compares two standalone TypeScript declarations:

- `src/fluent/main.ts`: queues in a typed `Record`, merged defaults, and an immutable fluent API.
- `src/functional/main.ts`: a string union, composition with `pipe(..., [...])`, and explicit declarative configuration.

Each example contains five `users` consumers with distinct execution policies.

```sh
npm run build
```
