type RetryPolicy = { readonly maxAttempts: number; readonly delayMs: number; readonly backoff: "fixed" | "exponential" }
type Decoder<T> = (input: unknown) => { readonly ok: true; readonly value: T } | { readonly ok: false; readonly errors: readonly string[] }
type MessageMetadata = { readonly messageId?: string; readonly attempt: number; readonly headers: Readonly<Record<string, unknown>> }

// Cette version ne possède pas de Record de queues : l'union est l'autorité de typage.
type QueueName = "users.create" | "users.update" | "users.delete" | "users.archive" | "users.purge";
type RetrySetting = false | RetryPolicy;
type Execution = { readonly concurrency: number; readonly timeoutMs: number };

type Dependencies = {
  readonly users: {
    create(message: CreateUser): Promise<void>;
    update(message: UpdateUser): Promise<void>;
    delete(message: DeleteUser): Promise<void>;
  };
};
type HandlerContext<Message> = {
  readonly message: Message;
  readonly dependencies: Dependencies;
  readonly metadata: MessageMetadata;
};
type Consumer<Q extends QueueName, Message = unknown> = {
  readonly queue: Q;
  readonly decoder?: Decoder<Message>;
  readonly execution?: Execution;
  readonly retry?: RetrySetting;
  readonly deadLetterQueue?: QueueName;
  readonly handler?: (context: HandlerContext<Message>) => void | Promise<void>;
};

// Politique globale : chaque consumer reste explicite et peut la réutiliser ou la remplacer.
const defaultRetry: RetryPolicy = { maxAttempts: 3, delayMs: 1_000, backoff: "exponential" };

const consumer = <Q extends QueueName>(queue: Q): Consumer<Q> => ({ queue });
const validate = <Message>(decoder: Decoder<Message>) => <Q extends QueueName>(value: Consumer<Q>): Consumer<Q, Message> =>
  ({ ...value, decoder });
const withExecution = (execution: Execution) => <Q extends QueueName, Message>(value: Consumer<Q, Message>): Consumer<Q, Message> =>
  ({ ...value, execution });
const withRetry = (retry: RetrySetting) => <Q extends QueueName, Message>(value: Consumer<Q, Message>): Consumer<Q, Message> =>
  ({ ...value, retry });
const withDeadLetterQueue = (deadLetterQueue: QueueName) => <Q extends QueueName, Message>(value: Consumer<Q, Message>): Consumer<Q, Message> =>
  ({ ...value, deadLetterQueue });
const handle = <Message>(handler: (context: HandlerContext<Message>) => void | Promise<void>) =>
  <Q extends QueueName>(value: Consumer<Q, Message>): Consumer<Q, Message> => ({ ...value, handler });

type Step = (value: any) => any;

// Un tableau rend l'enchaînement extensible sans multiplier les surcharges TypeScript.
const pipe = <Value>(value: Value, steps: readonly Step[]): unknown =>
  steps.reduce((current, step) => step(current), value);

type CreateUser = { readonly userId: string; readonly email: string };
type UpdateUser = { readonly userId: string; readonly email: string };
type DeleteUser = { readonly userId: string };
const createUserDecoder: Decoder<CreateUser> = (input) => ({ ok: true, value: input as CreateUser });
const updateUserDecoder: Decoder<UpdateUser> = (input) => ({ ok: true, value: input as UpdateUser });
const deleteUserDecoder: Decoder<DeleteUser> = (input) => ({ ok: true, value: input as DeleteUser });

// Un handler résout normalement : le runtime acquitte. Une exception déclenche son retry.
export const usersConsumers = {
  createUserConsumer: pipe(
    consumer("users.create"),
    [
      validate(createUserDecoder),
      withExecution({ concurrency: 5, timeoutMs: 30_000 }),
      withRetry(defaultRetry),
      withDeadLetterQueue("users.archive"),
      handle<CreateUser>(async ({ message, dependencies, metadata }) => {
      await dependencies.users.create(message);
      console.info("created", metadata.messageId);
      }),
    ],
  ),
  updateUserConsumer: pipe(
    consumer("users.update"),
    [
      validate(updateUserDecoder),
      withExecution({ concurrency: 1, timeoutMs: 5_000 }),
      withRetry({ maxAttempts: 5, delayMs: 500, backoff: "fixed" }),
      handle<UpdateUser>(async ({ message, dependencies }) => { await dependencies.users.update(message); }),
    ],
  ),
  deleteUserConsumer: pipe(
    consumer("users.delete"),
    [
      validate(deleteUserDecoder),
      withExecution({ concurrency: 2, timeoutMs: 10_000 }),
      withRetry(false),
      handle<DeleteUser>(async ({ message, dependencies }) => { await dependencies.users.delete(message); }),
    ],
  ),
  archiveUserConsumer: pipe(
    consumer("users.archive"),
    [
      validate(createUserDecoder),
      withExecution({ concurrency: 1, timeoutMs: 60_000 }),
      withRetry(defaultRetry),
      handle<CreateUser>(async ({ metadata }) => { console.info("archived", metadata.messageId); }),
    ],
  ),
  purgeUserConsumer: pipe(
    consumer("users.purge"),
    [
      validate(deleteUserDecoder),
      withExecution({ concurrency: 1, timeoutMs: 2_000 }),
      withRetry(false),
      handle<DeleteUser>(async ({ metadata }) => { console.info("purged", metadata.messageId); }),
    ],
  ),
};
