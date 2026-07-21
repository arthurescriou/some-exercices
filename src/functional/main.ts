type RetryPolicy = { readonly maxAttempts: number; readonly delayMs: number; readonly backoff: "fixed" | "exponential" }
type Decoder<T> = (input: unknown) => { readonly ok: true; readonly value: T } | { readonly ok: false; readonly errors: readonly string[] }
type MessageMetadata = { readonly messageId?: string; readonly attempt: number; readonly headers: Readonly<Record<string, unknown>> }

// This version has no queue Record: the union is the typing authority.
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

// Global policy: each consumer remains explicit and can reuse or override it.
const defaultRetry: RetryPolicy = { maxAttempts: 3, delayMs: 1_000, backoff: "exponential" };

const consumer = <Q extends QueueName>(queue: Q): Consumer<Q> => ({ queue });
const validate = <Message>(decoder: Decoder<Message>) => <Q extends QueueName>(value: Consumer<Q>): Consumer<Q, Message> =>
  ({ ...value, decoder });
type ConsumerConfiguration = Pick<Consumer<QueueName>, "execution" | "retry" | "deadLetterQueue">
const configure = (configuration: Partial<ConsumerConfiguration>) =>
  <Q extends QueueName, Message>(value: Consumer<Q, Message>): Consumer<Q, Message> =>
    ({ ...value, ...configuration });
const handle = <Message>(handler: (context: HandlerContext<Message>) => void | Promise<void>) =>
  <Q extends QueueName>(value: Consumer<Q, Message>): Consumer<Q, Message> => ({ ...value, handler });

type Step = (value: any) => any;

// An array keeps the pipeline extensible without multiplying TypeScript overloads.
const pipe = <Value>(value: Value, steps: readonly Step[]): unknown =>
  steps.reduce((current, step) => step(current), value);

type CreateUser = { readonly userId: string; readonly email: string };
type UpdateUser = { readonly userId: string; readonly email: string };
type DeleteUser = { readonly userId: string };
const createUserDecoder: Decoder<CreateUser> = (input) => ({ ok: true, value: input as CreateUser });
const updateUserDecoder: Decoder<UpdateUser> = (input) => ({ ok: true, value: input as UpdateUser });
const deleteUserDecoder: Decoder<DeleteUser> = (input) => ({ ok: true, value: input as DeleteUser });

// A handler normally resolves and the runtime acknowledges it. An exception triggers a retry.
export const usersConsumers = {
  createUserConsumer: pipe(
    consumer("users.create"),
    [
      validate(createUserDecoder),
      configure({
        execution: { concurrency: 5, timeoutMs: 30_000 },
        retry: defaultRetry,
        deadLetterQueue: "users.archive",
      }),
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
      configure({
        execution: { concurrency: 1, timeoutMs: 5_000 },
        retry: { maxAttempts: 5, delayMs: 500, backoff: "fixed" },
      }),
      handle<UpdateUser>(async ({ message, dependencies }) => { await dependencies.users.update(message); }),
    ],
  ),
  deleteUserConsumer: pipe(
    consumer("users.delete"),
    [
      validate(deleteUserDecoder),
      configure({
        execution: { concurrency: 2, timeoutMs: 10_000 },
        retry: false,
      }),
      handle<DeleteUser>(async ({ message, dependencies }) => { await dependencies.users.delete(message); }),
    ],
  ),
  archiveUserConsumer: pipe(
    consumer("users.archive"),
    [
      validate(createUserDecoder),
      configure({
        execution: { concurrency: 1, timeoutMs: 60_000 },
        retry: defaultRetry,
      }),
      handle<CreateUser>(async ({ metadata }) => { console.info("archived", metadata.messageId); }),
    ],
  ),
  purgeUserConsumer: pipe(
    consumer("users.purge"),
    [
      validate(deleteUserDecoder),
      configure({
        execution: { concurrency: 1, timeoutMs: 2_000 },
        retry: false,
      }),
      handle<DeleteUser>(async ({ metadata }) => { console.info("purged", metadata.messageId); }),
    ],
  ),
};
