import type { ApiRequest, IIIClient, ISdk, StreamRequest } from '../src/index'

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2 ? true : false
type Expect<T extends true> = T
type RequestBody = { value: string }

export type LegacyTypeCompatibility = [
  Expect<Equal<ISdk, IIIClient>>,
  Expect<Equal<ReturnType<ISdk['shutdown']>, Promise<void>>>,
  Expect<Equal<ApiRequest<RequestBody>, StreamRequest<RequestBody>>>,
  Expect<Equal<ApiRequest<RequestBody>['body'], RequestBody>>,
  Expect<Equal<'response' extends keyof ApiRequest<RequestBody> ? true : false, false>>,
]
