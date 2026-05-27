package controller

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	var out []string
	for _, item := range slice {
		if item != s {
			out = append(out, item)
		}
	}
	return out
}
