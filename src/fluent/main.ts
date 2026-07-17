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

interface RawBuilder<Queue extends string> extends BuilderOptions<RawBuilder<Queue>> {
  decode<Message>(decoder: Decoder<Message>): DecodedBuilder<Queue, Message>
}
interface DecodedBuilder<Queue extends string, Message> extends BuilderOptions<DecodedBuilder<Queue, Message>> {
  handle(handler: (message: Message) => HandlerResult | Promise<HandlerResult>): ReadyBuilder<Queue, Message>
}
interface ReadyBuilder<Queue extends string, Message> extends BuilderOptions<ReadyBuilder<Queue, Message>> { build(): Consumer<Queue, Message> }
interface BuilderOptions<Next> {
  withConcurrency(value: number): Next
  withTimeout(value: number): Next
  withRetry(value: RetryPolicy): Next
  withoutRetry(): Next
  withDeadLetterQueue(queue: string): Next
}
type Consumer<Queue extends string, Message> = Readonly<{ queue: Queue; decoder: Decoder<Message>; handler: (message: Message) => HandlerResult | Promise<HandlerResult>; options: Options }>
type State = { readonly queue: string; readonly options: Options; readonly decoder?: Decoder<unknown>; readonly handler?: (message: unknown) => HandlerResult | Promise<HandlerResult> }

const createConsumerBuilder = <Queue extends string>(defaults: Omit<Options, "deadLetterQueue">) => (queue: Queue): RawBuilder<Queue> => {
  const make = (state: State): object => {
    const options = {
      withConcurrency: (concurrency: number) => make({ ...state, options: { ...state.options, concurrency } }),
      withTimeout: (timeoutMs: number) => make({ ...state, options: { ...state.options, timeoutMs } }),
      withRetry: (retry: RetryPolicy) => make({ ...state, options: { ...state.options, retry } }),
      withoutRetry: () => make({ ...state, options: { ...state.options, retry: false } }),
      withDeadLetterQueue: (deadLetterQueue: string) => make({ ...state, options: { ...state.options, deadLetterQueue } }),
    }
    if (!state.decoder) return { ...options, decode: (decoder: Decoder<unknown>) => make({ ...state, decoder }) }
    if (!state.handler) return { ...options, handle: (handler: (message: unknown) => HandlerResult | Promise<HandlerResult>) => make({ ...state, handler }) }
    return { ...options, build: () => Object.freeze({ ...state }) }
  }
  return make({ queue, options: { ...defaults } }) as RawBuilder<Queue>
}

// Le Record est la source de vérité des noms de queues.
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

// Les dépendances sont volontairement capturées par closure : elles ne font pas partie du handler.
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

// Module de domaine : les cinq consumers sont regroupés sous usersConsumers.
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
