package util

// Check verifies the given error, and panic if it is not nil.
func Check(err error) {
	if err != nil {
		panic(err)
	}
}
