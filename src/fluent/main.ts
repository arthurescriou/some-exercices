type RetryPolicy = { readonly maxAttempts: number; readonly delayMs: number; readonly backoff: "fixed" | "exponential" }
type Decoder<T> = (input: unknown) => { readonly ok: true; readonly value: T } | { readonly ok: false; readonly errors: readonly string[] }
type JoiSchema<T> = { validate(input: unknown): { readonly value: T; readonly error?: { readonly message: string } } }
type HandlerResult = { readonly kind: "ack" | "retry" | "dead-letter"; readonly error?: unknown }
type Options = { readonly concurrency: number; readonly prefetch: number; readonly timeoutMs: number; readonly retry: RetryPolicy | false; readonly deadLetterQueue?: string }

const defineQueues = <const T extends Record<string, string>>(queues: T): Readonly<T> => Object.freeze({ ...queues })
const fromJoi = <T>(schema: JoiSchema<T>): Decoder<T> => (input) => {
  const result = schema.validate(input)
  return result.error ? { ok: false, errors: [result.error.message] } : { ok: true, value: result.value }
}
const ack = (): HandlerResult => ({ kind: "ack" })
const retry = (): HandlerResult => ({ kind: "retry" })
const deadLetter = (error: unknown): HandlerResult => ({ kind: "dead-letter", error })

type Stage = "raw" | "decoded" | "ready"
type Builder<Queue extends string, Message, CurrentStage extends Stage> = {
  withConcurrency(value: number): Builder<Queue, Message, CurrentStage>
  withTimeout(value: number): Builder<Queue, Message, CurrentStage>
  withRetry(value: RetryPolicy): Builder<Queue, Message, CurrentStage>
  withoutRetry(): Builder<Queue, Message, CurrentStage>
  withDeadLetterQueue(queue: string): Builder<Queue, Message, CurrentStage>
} & (CurrentStage extends "raw"
  ? { decode<Decoded>(decoder: Decoder<Decoded>): Builder<Queue, Decoded, "decoded"> }
  : CurrentStage extends "decoded"
    ? { handle(handler: (message: Message) => HandlerResult | Promise<HandlerResult>): Builder<Queue, Message, "ready"> }
    : { build(): Consumer<Queue, Message> })
type Consumer<Queue extends string, Message> = Readonly<{ queue: Queue; decoder: Decoder<Message>; handler: (message: Message) => HandlerResult | Promise<HandlerResult>; options: Options }>
type State = { readonly queue: string; readonly options: Options; readonly decoder?: Decoder<unknown>; readonly handler?: (message: unknown) => HandlerResult | Promise<HandlerResult> }

const createConsumerBuilder = <Queue extends string>(defaults: Omit<Options, "deadLetterQueue">) => (queue: Queue): Builder<Queue, unknown, "raw"> => {
  const make = <Message, CurrentStage extends Stage>(state: State, stage: CurrentStage): Builder<Queue, Message, CurrentStage> => {
    const options = {
      withConcurrency: (concurrency: number) => make({ ...state, options: { ...state.options, concurrency } }, stage),
      withTimeout: (timeoutMs: number) => make({ ...state, options: { ...state.options, timeoutMs } }, stage),
      withRetry: (retry: RetryPolicy) => make({ ...state, options: { ...state.options, retry } }, stage),
      withoutRetry: () => make({ ...state, options: { ...state.options, retry: false } }, stage),
      withDeadLetterQueue: (deadLetterQueue: string) => make({ ...state, options: { ...state.options, deadLetterQueue } }, stage),
    }
    if (stage === "raw") {
      return { ...options, decode: <Decoded>(decoder: Decoder<Decoded>) => make<Decoded, "decoded">({ ...state, decoder }, "decoded") } as unknown as Builder<Queue, Message, CurrentStage>
    }
    if (stage === "decoded") {
      return { ...options, handle: (handler: (message: Message) => HandlerResult | Promise<HandlerResult>) => make<Message, "ready">({ ...state, handler: handler as (message: unknown) => HandlerResult | Promise<HandlerResult> }, "ready") } as unknown as Builder<Queue, Message, CurrentStage>
    }
    return { ...options, build: () => Object.freeze({ ...state }) } as unknown as Builder<Queue, Message, CurrentStage>
  }
  return make<unknown, "raw">({ queue, options: { ...defaults } }, "raw")
}

// The Record is the source of truth for queue names.
export const Queues = defineQueues({
  usersCreate: "users.create",
  usersCreateFailed: "users.create.failed",
  usersUpdate: "users.update",
  usersUpdateFailed: "users.update.failed",
  usersDelete: "users.delete",
});
type QueueName = (typeof Queues)[keyof typeof Queues];

type CreateUser = { readonly userId: string; readonly email: string };
type UpdateUser = { readonly userId: string; readonly email: string };
type DeleteUser = { readonly userId: string };

const valid = <T>(value: T) => ({ value });
const createUserSchema: JoiSchema<CreateUser> = { validate: (input) => valid(input as CreateUser) };
const updateUserSchema: JoiSchema<UpdateUser> = { validate: (input) => valid(input as UpdateUser) };
const deleteUserSchema: JoiSchema<DeleteUser> = { validate: (input) => valid(input as DeleteUser) };

// Dependencies are deliberately captured by closure rather than passed to the handler.
const users = {
  create: async (_message: CreateUser) => undefined,
  update: async (_message: UpdateUser) => undefined,
  delete: async (_message: DeleteUser) => undefined,
};

const defineConsumer = createConsumerBuilder<QueueName>({
  concurrency: 5,
  prefetch: 10,
  timeoutMs: 30_000,
  retry: { maxAttempts: 3, delayMs: 1_000, backoff: "exponential" },
});

// Domain module: the five consumers are grouped under usersConsumers.
export const usersConsumers = {
  createUserConsumer: defineConsumer(Queues.usersCreate)
    .decode(fromJoi(createUserSchema))
    .handle(async (message) => {
      await users.create(message);
      return ack();
    })
    .withDeadLetterQueue(Queues.usersCreateFailed)
    .build(),

  createFailedConsumer: defineConsumer(Queues.usersCreateFailed)
    .decode(fromJoi(createUserSchema))
    .withConcurrency(1)
    .withTimeout(5_000)
    .withoutRetry()
    .handle(async (_message) => ack())
    .build(),

  updateUserConsumer: defineConsumer(Queues.usersUpdate)
    .decode(fromJoi(updateUserSchema))
    .withConcurrency(2)
    .withTimeout(15_000)
    .handle(async (message) => {
      await users.update(message);
      return ack();
    })
    .withDeadLetterQueue(Queues.usersUpdateFailed)
    .build(),

  updateFailedConsumer: defineConsumer(Queues.usersUpdateFailed)
    .decode(fromJoi(updateUserSchema))
    .withConcurrency(1)
    .withTimeout(10_000)
    .withRetry({ maxAttempts: 1, delayMs: 500, backoff: "fixed" })
    .handle(async (_message) => retry())
    .build(),

  deleteUserConsumer: defineConsumer(Queues.usersDelete)
    .decode(fromJoi(deleteUserSchema))
    .withConcurrency(3)
    .withTimeout(8_000)
    .withoutRetry()
    .handle(async (message) => {
      try {
        await users.delete(message);
        return ack();
      } catch (error) {
        return deadLetter(error);
      }
    })
    .build(),
};
