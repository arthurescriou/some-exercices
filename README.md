# Deux styles d’API RabbitMQ

Ce dépôt compare uniquement deux déclarations TypeScript autonomes :

- `src/fluent/main.ts` : queues dans un `Record`, defaults fusionnés et API fluent immuable.
- `src/functional/main.ts` : union de chaînes, composition avec `pipe(..., [...])` et configuration déclarative explicite.

Chaque exemple contient cinq consumers `users` avec des politiques d’exécution distinctes.

```sh
npm run build
```
