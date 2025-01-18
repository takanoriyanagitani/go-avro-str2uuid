package util

func ComposeErr[T, U, V any](
	fn0 func(T) (U, error),
	fn1 func(U) (V, error),
) func(T) (V, error) {
	return func(t T) (v V, e error) {
		u, e := fn0(t)
		switch e {
		case nil:
			return fn1(u)
		default:
			return v, e
		}
	}
}
